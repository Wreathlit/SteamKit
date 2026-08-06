using System;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using SteamKit2;
using Xunit;

namespace Tests
{
#if DEBUG
    public class WebSocketUriFacts
    {
        [Fact]
        public void DnsEndPoint()
        {
            var endpoint = new DnsEndPoint( "example.com", 1337 );
            Assert.Equal( "wss://example.com:1337/cmsocket/", WebSocketConnection.WebSocketContext.ConstructUri( endpoint ).ToString() );
        }

        [Fact]
        public void IPEndPointV4()
        {
            var endpoint = new IPEndPoint( IPAddress.Loopback, 1337 );
            Assert.Equal( "wss://127.0.0.1:1337/cmsocket/", WebSocketConnection.WebSocketContext.ConstructUri( endpoint ).ToString() );
        }

        [Fact]
        public void IPEndPointV6()
        {
            var endpoint = new IPEndPoint( IPAddress.IPv6Loopback, 1337 );
            Assert.Equal( "wss://[::1]:1337/cmsocket/", WebSocketConnection.WebSocketContext.ConstructUri( endpoint ).ToString() );
        }

        [Fact]
        public void ThrowsWrongEndPoint()
        {
            Assert.Throws<InvalidOperationException>( () => WebSocketConnection.WebSocketContext.ConstructUri( new DummyEndPoint() ) );
        }

        [Fact]
        public void TransportFailureDiagnosticIdentifiesOperationEndpointAndWebSocketReason()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            var log = new CapturingLogContext();
            using var connection = new WebSocketConnection(log, invoker);
            using var context = new WebSocketConnection.WebSocketContext(
                connection,
                new DnsEndPoint("example.com", 443));

            InvokeLogTransportFailure(
                context,
                "read",
                new WebSocketException(WebSocketError.ConnectionClosedPrematurely));

            Assert.Equal(nameof(WebSocketConnection.WebSocketContext), log.Category);
            Assert.Contains("operation=read", log.Message);
            Assert.Contains("reason=exception", log.Message);
            Assert.Contains("context=", log.Message);
            Assert.Contains("endpoint=wss://example.com/cmsocket/", log.Message);
            Assert.Contains(
                "websocket_error=ConnectionClosedPrematurely",
                log.Message);
        }

        [Fact]
        public void ContextDisposeAlwaysDisposesSocketWhenCancellationFails()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            using var context = new WebSocketConnection.WebSocketContext(
                connection,
                new DnsEndPoint("example.com", 443));
            var cancellation = GetPrivateField<CancellationTokenSource>(context, "cts");
            var socket = GetPrivateField<ClientWebSocket>(context, "socket");
            _ = cancellation.Token.Register(
                static () => throw new InvalidOperationException("test cancellation failure"));

            Assert.Throws<AggregateException>(() => context.Dispose());

            Assert.Equal(WebSocketState.Closed, socket.State);
        }

        [Fact]
        public void DisconnectCompletesLogicalStateWhenContextDisposeFails()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            var endpoint = new DnsEndPoint("example.com", 443);
            using var context = new WebSocketConnection.WebSocketContext(connection, endpoint);
            var cancellation = GetPrivateField<CancellationTokenSource>(context, "cts");
            _ = cancellation.Token.Register(
                static () => throw new InvalidOperationException("test cancellation failure"));
            typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, context);
            connection.CurrentEndPoint = endpoint;

            var disconnected = false;
            var userInitiated = false;
            EndPoint endpointDuringCallback = null;
            connection.Disconnected += (_, args) =>
            {
                disconnected = true;
                userInitiated = args.UserInitiated;
                endpointDuringCallback = connection.CurrentEndPoint;
            };

            Assert.Throws<AggregateException>(() => connection.Disconnect(userInitiated: true));

            Assert.True(disconnected);
            Assert.True(userInitiated);
            Assert.Equal(endpoint, endpointDuringCallback);
            Assert.Null(connection.CurrentEndPoint);
        }

        [Fact]
        public async Task ExplicitDisconnectWaitsForRunLoopLogicalDisconnectWithoutDuplicatingCallback()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            var endpoint = new DnsEndPoint("example.com", 443);
            using var context = new WebSocketConnection.WebSocketContext(connection, endpoint);
            var cancellation = GetPrivateField<CancellationTokenSource>(context, "cts");
            using var disposeEntered = new ManualResetEventSlim();
            using var releaseDispose = new ManualResetEventSlim();
            using var waitStarted = new ManualResetEventSlim();
            using var registration = cancellation.Token.Register(() =>
            {
                disposeEntered.Set();
                releaseDispose.Wait();
            });
            typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, context);
            connection.CurrentEndPoint = endpoint;
            connection.LogicalDisconnectWaitStartedForTesting = waitStarted.Set;
            var disconnectedCount = 0;
            connection.Disconnected += (_, _) => Interlocked.Increment(ref disconnectedCount);
            var testCancellation = TestContext.Current.CancellationToken;

            Task runLoopDisconnect = null;
            Task explicitDisconnect = null;
            try
            {
                runLoopDisconnect = Task.Run(
                    () => InvokeDisconnectCore(connection, false, context),
                    testCancellation);
                Assert.True(disposeEntered.Wait(TimeSpan.FromSeconds(5), testCancellation));

                explicitDisconnect = Task.Run(
                    () => connection.Disconnect(userInitiated: true),
                    testCancellation);
                Assert.True(waitStarted.Wait(TimeSpan.FromSeconds(5), testCancellation));
                Assert.False(explicitDisconnect.IsCompleted);

                releaseDispose.Set();
                await runLoopDisconnect.WaitAsync(TimeSpan.FromSeconds(5), testCancellation);
                await explicitDisconnect.WaitAsync(TimeSpan.FromSeconds(5), testCancellation);

                Assert.Equal(1, Volatile.Read(ref disconnectedCount));
                Assert.Null(connection.CurrentEndPoint);
            }
            finally
            {
                releaseDispose.Set();
            }
        }

        [Fact]
        public async Task ConnectWaitsForPriorLogicalDisconnectBeforeInstallingNewContext()
        {
            using var handler = new BlockingHttpMessageHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            var oldEndpoint = new DnsEndPoint("old.example.com", 443);
            var newEndpoint = new DnsEndPoint("new.example.com", 443);
            using var oldContext = new WebSocketConnection.WebSocketContext(connection, oldEndpoint);
            var cancellation = GetPrivateField<CancellationTokenSource>(oldContext, "cts");
            using var disposeEntered = new ManualResetEventSlim();
            using var releaseDispose = new ManualResetEventSlim();
            using var waitStarted = new ManualResetEventSlim();
            using var registration = cancellation.Token.Register(() =>
            {
                disposeEntered.Set();
                releaseDispose.Wait();
            });
            typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, oldContext);
            connection.CurrentEndPoint = oldEndpoint;
            connection.LogicalDisconnectWaitStartedForTesting = waitStarted.Set;
            var disconnectedCount = 0;
            connection.Disconnected += (_, _) => Interlocked.Increment(ref disconnectedCount);
            var testCancellation = TestContext.Current.CancellationToken;

            Task runLoopDisconnect = null;
            Task reconnect = null;
            try
            {
                runLoopDisconnect = Task.Run(
                    () => InvokeDisconnectCore(connection, false, oldContext),
                    testCancellation);
                Assert.True(disposeEntered.Wait(TimeSpan.FromSeconds(5), testCancellation));

                reconnect = Task.Run(
                    () => connection.Connect(newEndpoint, timeout: 30_000),
                    testCancellation);
                Assert.True(waitStarted.Wait(TimeSpan.FromSeconds(5), testCancellation));
                Assert.False(reconnect.IsCompleted);
                Assert.Null(GetPrivateFieldOrNull<WebSocketConnection.WebSocketContext>(connection, "currentContext"));

                releaseDispose.Set();
                await runLoopDisconnect.WaitAsync(TimeSpan.FromSeconds(5), testCancellation);
                await reconnect.WaitAsync(TimeSpan.FromSeconds(5), testCancellation);
                Assert.True(handler.RequestStarted.Wait(TimeSpan.FromSeconds(5), testCancellation));

                var newContext = GetPrivateField<WebSocketConnection.WebSocketContext>(connection, "currentContext");
                Assert.NotSame(oldContext, newContext);
                Assert.Equal(newEndpoint, connection.CurrentEndPoint);
                Assert.Equal(1, Volatile.Read(ref disconnectedCount));

                connection.Disconnect(userInitiated: true);
                Assert.Equal(2, Volatile.Read(ref disconnectedCount));
            }
            finally
            {
                releaseDispose.Set();
            }
        }

        [Fact]
        public void ConnectedNotificationRequiresCurrentNonCancelledContext()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            var endpoint = new DnsEndPoint("example.com", 443);
            using var currentContext = new WebSocketConnection.WebSocketContext(connection, endpoint);
            using var staleContext = new WebSocketConnection.WebSocketContext(connection, endpoint);
            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, currentContext);
            connection.CurrentEndPoint = endpoint;
            var connectedCount = 0;
            connection.Connected += (_, _) => connectedCount++;
            var connectionUri = new Uri("wss://example.com/cmsocket/");

            Assert.False(InvokeTryRaiseConnected(
                connection,
                staleContext,
                CancellationToken.None,
                connectionUri));
            Assert.False(InvokeTryRaiseConnected(
                connection,
                currentContext,
                cancelled.Token,
                connectionUri));
            Assert.True(InvokeTryRaiseConnected(
                connection,
                currentContext,
                CancellationToken.None,
                connectionUri));

            Assert.Equal(1, connectedCount);
            Assert.Same(currentContext, GetPrivateField<WebSocketConnection.WebSocketContext>(
                connection,
                "currentContext"));

            connection.Disconnect(userInitiated: true);
        }

        [Fact]
        public async Task ConnectedNotificationCompletesBeforeCrossThreadDisconnectNotification()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            var endpoint = new DnsEndPoint("example.com", 443);
            using var context = new WebSocketConnection.WebSocketContext(connection, endpoint);
            typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, context);
            connection.CurrentEndPoint = endpoint;
            using var connectedEntered = new ManualResetEventSlim();
            using var releaseConnected = new ManualResetEventSlim();
            using var disconnectWaitStarted = new ManualResetEventSlim();
            connection.ConnectedNotificationWaitStartedForTesting = disconnectWaitStarted.Set;
            var sequence = 0;
            var connectedOrder = 0;
            var disconnectedOrder = 0;
            var connectedCount = 0;
            var disconnectedCount = 0;
            connection.Connected += (_, _) =>
            {
                Interlocked.Increment(ref connectedCount);
                connectedOrder = Interlocked.Increment(ref sequence);
                connectedEntered.Set();
                releaseConnected.Wait();
            };
            connection.Disconnected += (_, _) =>
            {
                Interlocked.Increment(ref disconnectedCount);
                disconnectedOrder = Interlocked.Increment(ref sequence);
            };
            var testCancellation = TestContext.Current.CancellationToken;
            Task connectedNotification = null;
            Task disconnect = null;

            try
            {
                connectedNotification = Task.Run(
                    () => Assert.True(InvokeTryRaiseConnected(
                        connection,
                        context,
                        CancellationToken.None,
                        new Uri("wss://example.com/cmsocket/"))),
                    testCancellation);
                Assert.True(connectedEntered.Wait(TimeSpan.FromSeconds(5), testCancellation));

                disconnect = Task.Run(
                    () => connection.Disconnect(userInitiated: true),
                    testCancellation);
                Assert.True(disconnectWaitStarted.Wait(TimeSpan.FromSeconds(5), testCancellation));
                Assert.False(disconnect.IsCompleted);
                Assert.Equal(0, Volatile.Read(ref disconnectedCount));
                Assert.Same(context, GetPrivateField<WebSocketConnection.WebSocketContext>(
                    connection,
                    "currentContext"));

                releaseConnected.Set();
                await Task.WhenAll(connectedNotification, disconnect)
                    .WaitAsync(TimeSpan.FromSeconds(5), testCancellation);

                Assert.Equal(1, Volatile.Read(ref connectedCount));
                Assert.Equal(1, Volatile.Read(ref disconnectedCount));
                Assert.Equal(1, connectedOrder);
                Assert.Equal(2, disconnectedOrder);
                Assert.Null(connection.CurrentEndPoint);
            }
            finally
            {
                releaseConnected.Set();
            }
        }

        [Fact]
        public void SendFailureFromSupersededContextDoesNotDisconnectCurrentContext()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            var oldEndpoint = new DnsEndPoint("old.example.com", 443);
            var newEndpoint = new DnsEndPoint("new.example.com", 443);
            using var oldContext = new WebSocketConnection.WebSocketContext(connection, oldEndpoint);
            using var newContext = new WebSocketConnection.WebSocketContext(connection, newEndpoint);
            var currentContextField = typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!;
            currentContextField.SetValue(connection, oldContext);
            connection.CurrentEndPoint = oldEndpoint;
            var disconnectedCount = 0;
            connection.Disconnected += (_, _) => disconnectedCount++;
            connection.SendFailureObservedForTesting = () =>
            {
                currentContextField.SetValue(connection, newContext);
                connection.CurrentEndPoint = newEndpoint;
            };

            connection.Send(new byte[] { 1 });

            Assert.Same(newContext, GetPrivateField<WebSocketConnection.WebSocketContext>(
                connection,
                "currentContext"));
            Assert.Equal(newEndpoint, connection.CurrentEndPoint);
            Assert.Equal(0, disconnectedCount);
            Assert.Equal(
                WebSocketState.Closed,
                GetPrivateField<ClientWebSocket>(oldContext, "socket").State);
            Assert.NotEqual(
                WebSocketState.Closed,
                GetPrivateField<ClientWebSocket>(newContext, "socket").State);

            connection.SendFailureObservedForTesting = null;
            connection.Disconnect(userInitiated: true);
            Assert.Equal(1, disconnectedCount);
        }

        [Fact]
        public void StaleContextDisconnectDoesNotClearCurrentContext()
        {
            using var handler = new SocketsHttpHandler();
            using var invoker = new HttpMessageInvoker(handler, disposeHandler: false);
            using var connection = new WebSocketConnection(
                DebugLogContext.Instance,
                invoker);
            var endpoint = new DnsEndPoint("example.com", 443);
            using var currentContext = new WebSocketConnection.WebSocketContext(connection, endpoint);
            using var staleContext = new WebSocketConnection.WebSocketContext(connection, endpoint);
            typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(connection, currentContext);
            connection.CurrentEndPoint = endpoint;
            var disconnectedCount = 0;
            connection.Disconnected += (_, _) => disconnectedCount++;

            typeof(WebSocketConnection)
                .GetMethod("DisconnectCore", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(connection, new object[] { false, staleContext });

            Assert.Same(currentContext, GetPrivateField<WebSocketConnection.WebSocketContext>(
                connection,
                "currentContext"));
            Assert.Equal(endpoint, connection.CurrentEndPoint);
            Assert.Equal(0, disconnectedCount);
            Assert.Equal(
                WebSocketState.Closed,
                GetPrivateField<ClientWebSocket>(staleContext, "socket").State);
            Assert.NotEqual(
                WebSocketState.Closed,
                GetPrivateField<ClientWebSocket>(currentContext, "socket").State);

            connection.Disconnect(userInitiated: true);
            Assert.Equal(1, disconnectedCount);
        }

        static T GetPrivateField<T>(object instance, string fieldName)
            where T : class
            => Assert.IsType<T>(instance.GetType()
                .GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(instance));

        static T GetPrivateFieldOrNull<T>(object instance, string fieldName)
            where T : class
            => (T)instance.GetType()
                .GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(instance);

        static void InvokeDisconnectCore(
            WebSocketConnection connection,
            bool userInitiated,
            WebSocketConnection.WebSocketContext context)
            => typeof(WebSocketConnection)
                .GetMethod("DisconnectCore", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(connection, new object[] { userInitiated, context });

        static bool InvokeTryRaiseConnected(
            WebSocketConnection connection,
            WebSocketConnection.WebSocketContext context,
            CancellationToken cancellationToken,
            Uri connectionUri)
            => Assert.IsType<bool>(typeof(WebSocketConnection)
                .GetMethod("TryRaiseConnected", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(connection, new object[] { context, cancellationToken, connectionUri }));

        static void InvokeLogTransportFailure(
            WebSocketConnection.WebSocketContext context,
            string operation,
            Exception exception)
            => typeof(WebSocketConnection.WebSocketContext)
                .GetMethod("LogTransportFailure", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(context, new object[] { operation, exception });

        sealed class CapturingLogContext : ILogContext
        {
            public string Category { get; private set; }
            public string Message { get; private set; }

            public void LogDebug(string category, string message, params object[] args)
            {
                Category = category;
                Message = string.Format(
                    CultureInfo.InvariantCulture,
                    message,
                    args ?? Array.Empty<object>());
            }
        }

        sealed class BlockingHttpMessageHandler : HttpMessageHandler
        {
            public ManualResetEventSlim RequestStarted { get; } = new();

            protected override async Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                RequestStarted.Set();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException("The blocking handler should only complete through cancellation.");
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    RequestStarted.Dispose();
                }

                base.Dispose(disposing);
            }
        }

        class DummyEndPoint : EndPoint
        {
        }
    }
#endif
}
