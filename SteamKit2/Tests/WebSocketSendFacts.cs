using System;
using System.Collections.Concurrent;
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
    public class WebSocketSendFacts
    {
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task FirstSendFailureWithinConnectedNotificationDoesNotDeadlock(bool timeout)
        {
            using var fixture = new SendFixture();
            fixture.Context.SendTimeoutForTesting = TimeSpan.FromMilliseconds(50);
            fixture.Context.SocketSendOverrideForTesting = async (_, token) =>
            {
                if (timeout)
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                else
                {
                    // Force a continuation rather than a synchronous exception, as with
                    // a failed real network write from CMClient's first Connected send.
                    await Task.Yield();
                    throw new WebSocketException(WebSocketError.ConnectionClosedPrematurely);
                }
            };
            var callbackReturned = false;
            fixture.Connection.Connected += (_, _) =>
            {
                fixture.Connection.Send(new byte[] { 1 });
                callbackReturned = true;
            };
            var connected = Task.Run(() => typeof(WebSocketConnection)
                .GetMethod("TryRaiseConnected", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(fixture.Connection, new object[]
                {
                    fixture.Context, CancellationToken.None, new Uri("wss://example.com/cmsocket/")
                }));
            try
            {
                await connected.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                Assert.True(callbackReturned);
                Assert.Null(fixture.CurrentContext);
                Assert.Null(fixture.Connection.CurrentEndPoint);
                Assert.Equal(1, fixture.DisconnectCount);
                Assert.False(fixture.LastUserInitiated);
                Assert.Equal(WebSocketState.Closed, fixture.Socket(fixture.Context).State);
                Assert.Contains(fixture.Log.Messages, message => message.Contains(timeout
                    ? "operation=send; reason=timeout"
                    : "operation=send; reason=exception"));
            }
            finally
            {
                // On the regression variant, break only the test's deadlocked fence so
                // failure cannot strand a worker or hang fixture cleanup. The timed wait
                // above has already failed; this escape cannot make the test pass.
                var notificationField = typeof(WebSocketConnection)
                    .GetField("connectedNotification", BindingFlags.Instance | BindingFlags.NonPublic)!;
                var contextLock = typeof(WebSocketConnection)
                    .GetField("contextLock", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(fixture.Connection);
                lock (contextLock)
                {
                    var notification = notificationField.GetValue(fixture.Connection);
                    if (notification != null)
                    {
                        notificationField.SetValue(fixture.Connection, null);
                        ((TaskCompletionSource)notification.GetType().GetProperty("Completion")!
                            .GetValue(notification)).TrySetResult();
                    }
                }
                await connected.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            }
        }

        [Fact]
        public async Task SendDeadlineCancelsActualWriteAndCompletesLogicalDisconnect()
        {
            using var fixture = new SendFixture();
            var writeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            fixture.Context.SendTimeoutForTesting = TimeSpan.FromMilliseconds(50);
            fixture.Context.SocketSendOverrideForTesting = async (_, token) =>
            {
                writeStarted.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    cancelled.TrySetResult();
                    throw;
                }
            };
            // Use the synchronous transport entry used by SteamClient, not just the async
            // helper. Only the real socket-facing cancellation can release this call.
            var send = Task.Run(() => fixture.Connection.Send(new byte[] { 1 }), TestContext.Current.CancellationToken);
            try
            {
                await writeStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                await send.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                Assert.True(cancelled.Task.IsCompletedSuccessfully);
                Assert.Null(fixture.Connection.CurrentEndPoint);
                Assert.Null(fixture.CurrentContext);
                Assert.Equal(1, fixture.DisconnectCount);
                Assert.False(fixture.LastUserInitiated);
                Assert.Contains(fixture.Log.Messages, message => message.Contains("operation=send; reason=timeout"));
                Assert.Equal(WebSocketState.Closed, fixture.Socket(fixture.Context).State);
            }
            finally
            {
                fixture.Context.Dispose(); // also unblocks the regression variant with no send deadline
                await send.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            }
        }

        [Fact]
        public async Task SendTimeoutFromOldContextCannotDisconnectSuccessor()
        {
            using var fixture = new SendFixture();
            using var successor = new WebSocketConnection.WebSocketContext(fixture.Connection,
                new DnsEndPoint("successor.example.com", 443));
            fixture.Context.SendTimeoutForTesting = TimeSpan.FromMilliseconds(50);
            fixture.Context.SocketSendOverrideForTesting = async (_, token) =>
            {
                try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // Deterministic successor installation at the transport completion
                    // boundary, before the old send performs its timeout disconnect.
                    fixture.Install(successor);
                    throw;
                }
            };
            var send = Task.Run(() => fixture.Connection.Send(new byte[] { 1 }), TestContext.Current.CancellationToken);
            try
            {
                await send.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                Assert.Same(successor, fixture.CurrentContext);
                Assert.Equal(successor.EndPoint, fixture.Connection.CurrentEndPoint);
                Assert.Equal(0, fixture.DisconnectCount);
                Assert.Equal(WebSocketState.Closed, fixture.Socket(fixture.Context).State);
                Assert.NotEqual(WebSocketState.Closed, fixture.Socket(successor).State);
                Assert.Contains(fixture.Log.Messages, message => message.Contains("operation=send; reason=timeout"));
            }
            finally
            {
                fixture.Context.Dispose();
                await send.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                fixture.Connection.Disconnect(userInitiated: true);
            }
        }

        [Fact]
        public async Task ExplicitDisconnectCancelsWriteWithoutTimeoutOrDuplicateNotification()
        {
            using var fixture = new SendFixture();
            fixture.Context.SendTimeoutForTesting = TimeSpan.FromSeconds(30);
            var writeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            fixture.Context.SocketSendOverrideForTesting = async (_, token) =>
            {
                writeStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
            };
            var send = Task.Run(() => fixture.Connection.Send(new byte[] { 1 }), TestContext.Current.CancellationToken);
            try
            {
                await writeStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                fixture.Connection.Disconnect(userInitiated: true);
                await send.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
                Assert.Equal(1, fixture.DisconnectCount);
                Assert.True(fixture.LastUserInitiated);
                Assert.Null(fixture.CurrentContext);
                Assert.DoesNotContain(fixture.Log.Messages, message => message.Contains("operation=send; reason=timeout"));
            }
            finally
            {
                fixture.Context.Dispose();
                await send.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            }
        }

        [Fact]
        public void CompletedWriteKeepsConnectionUsable()
        {
            using var fixture = new SendFixture();
            var writes = 0;
            fixture.Context.SocketSendOverrideForTesting = (_, token) =>
            {
                Assert.True(token.CanBeCanceled);
                Assert.False(token.IsCancellationRequested);
                writes++;
                return ValueTask.CompletedTask;
            };

            fixture.Connection.Send(new byte[] { 1 });
            fixture.Connection.Send(new byte[] { 2 });

            Assert.Equal(2, writes);
            Assert.Same(fixture.Context, fixture.CurrentContext);
            Assert.Equal(0, fixture.DisconnectCount);
            Assert.Empty(fixture.Log.Messages);
        }

        sealed class SendFixture : IDisposable
        {
            readonly SocketsHttpHandler handler = new();
            readonly HttpMessageInvoker invoker;
            static readonly FieldInfo CurrentContextField = typeof(WebSocketConnection)
                .GetField("currentContext", BindingFlags.Instance | BindingFlags.NonPublic)!;
            int disconnectCount;
            public readonly CapturingLogContext Log = new();
            public readonly WebSocketConnection Connection;
            public readonly WebSocketConnection.WebSocketContext Context;
            public int DisconnectCount => Volatile.Read(ref disconnectCount);
            public bool LastUserInitiated { get; private set; }
            public WebSocketConnection.WebSocketContext CurrentContext
                => (WebSocketConnection.WebSocketContext)CurrentContextField.GetValue(Connection);

            public SendFixture()
            {
                invoker = new HttpMessageInvoker(handler, disposeHandler: false);
                Connection = new WebSocketConnection(Log, invoker);
                Context = new WebSocketConnection.WebSocketContext(Connection, new DnsEndPoint("example.com", 443));
                Install(Context);
                Connection.Disconnected += (_, args) =>
                {
                    LastUserInitiated = args.UserInitiated;
                    Interlocked.Increment(ref disconnectCount);
                };
            }

            public void Install(WebSocketConnection.WebSocketContext context)
            {
                CurrentContextField.SetValue(Connection, context);
                Connection.CurrentEndPoint = context.EndPoint;
            }

            public ClientWebSocket Socket(WebSocketConnection.WebSocketContext context)
                => (ClientWebSocket)typeof(WebSocketConnection.WebSocketContext)
                    .GetField("socket", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(context);

            public void Dispose()
            {
                Connection.Disconnect(userInitiated: true);
                Context.Dispose();
                Connection.Dispose();
                invoker.Dispose();
                handler.Dispose();
            }
        }

        sealed class CapturingLogContext : ILogContext
        {
            public readonly ConcurrentQueue<string> Messages = new();
            public void LogDebug(string category, string message, params object[] args)
                => Messages.Enqueue(string.Format(CultureInfo.InvariantCulture, message, args ?? Array.Empty<object>()));
        }
    }
#endif
}
