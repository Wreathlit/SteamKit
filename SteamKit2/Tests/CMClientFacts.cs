using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using SteamKit2;
using SteamKit2.Discovery;
using SteamKit2.Internal;
using Xunit;

namespace Tests
{
#if DEBUG
    [Collection( nameof( NotThreadSafeResourceCollection ) )]
    public class CMClientFacts
    {
        [Fact]
        public void GetPacketMsgReturnsPacketMsgForCryptoHandshake()
        {
            var messages = new[]
            {
                EMsg.ChannelEncryptRequest,
                EMsg.ChannelEncryptResponse,
                EMsg.ChannelEncryptResult
            };

            foreach (var emsg in messages)
            {
                var msgHdr = new MsgHdr { Msg = emsg };

                var data = Serialize(msgHdr);

                var packetMsg = CMClient.GetPacketMsg(data, DebugLogContext.Instance);
                Assert.IsAssignableFrom<PacketMsg>(packetMsg);
            }
        }

        [Fact]
        public void GetPacketMsgReturnsPacketClientMsgProtobufForMessagesWithProtomask()
        {
            var msg = MsgUtil.MakeMsg(EMsg.ClientLogOnResponse, protobuf: true);
            var msgHdr = new MsgHdrProtoBuf { Msg = msg };

            var data = Serialize(msgHdr);
            var packetMsg = CMClient.GetPacketMsg(data, DebugLogContext.Instance);
            Assert.IsAssignableFrom<PacketClientMsgProtobuf>(packetMsg);
        }

        [Fact]
        public void GetPacketMsgReturnsPacketClientMsgForOtherMessages()
        {
            var msg = MsgUtil.MakeMsg(EMsg.ClientLogOnResponse, protobuf: false);
            var msgHdr = new ExtendedClientMsgHdr { Msg = msg };

            var data = Serialize(msgHdr);
            var packetMsg = CMClient.GetPacketMsg(data, DebugLogContext.Instance);
            Assert.IsAssignableFrom<PacketClientMsg>(packetMsg);
        }

        [Fact]
        public void GetPacketMsgFailsWithNull()
        {
            var msg = MsgUtil.MakeMsg(EMsg.ClientLogOnResponse, protobuf: true);
            var msgHdr = new MsgHdrProtoBuf { Msg = msg };

            var data = Serialize(msgHdr);
            Array.Copy(BitConverter.GetBytes(-1), 0, data, 4, 4);
            var packetMsg = CMClient.GetPacketMsg(data, DebugLogContext.Instance);
            Assert.Null(packetMsg);
        }

        [Fact]
        public void GetPacketMsgFailsWithTinyArray()
        {
            var data = new byte[3];
            var packetMsg = CMClient.GetPacketMsg(data, DebugLogContext.Instance);
            Assert.Null(packetMsg);
        }

        [Fact]
        public void StaleDisconnectedSenderDoesNotReleaseCurrentConnectionGeneration()
        {
            var client = new TestCMClient();
            using var staleConnection = new TestConnection(new DnsEndPoint("stale.example.com", 443));
            using var currentConnection = new TestConnection(new DnsEndPoint("current.example.com", 443));
            SetConnection(client, currentConnection);
            client.SetIsConnected(true);

            InvokeDisconnected(client, staleConnection, userInitiated: false);

            Assert.Same(currentConnection, GetConnection(client));
            Assert.True(client.IsConnected);
            Assert.Equal(0, staleConnection.DisposeCount);
            Assert.Equal(0, currentConnection.DisposeCount);
            Assert.Equal(0, client.DisconnectedCount);

            InvokeDisconnected(client, currentConnection, userInitiated: true);

            Assert.Null(GetConnection(client));
            Assert.False(client.IsConnected);
            Assert.Equal(1, currentConnection.DisposeCount);
            Assert.Equal(1, client.DisconnectedCount);
        }

        [Fact]
        public void StaleConnectedSenderDoesNotPublishCurrentConnectionGeneration()
        {
            var client = new TestCMClient();
            using var staleConnection = new TestConnection(new DnsEndPoint("stale.example.com", 443));
            using var currentConnection = new TestConnection(new DnsEndPoint("current.example.com", 443));
            SetConnection(client, currentConnection);

            InvokeConnected(client, staleConnection);

            Assert.Same(currentConnection, GetConnection(client));
            Assert.False(client.IsConnected);
            Assert.Equal(0, client.ConnectedCount);

            InvokeConnected(client, currentConnection);

            Assert.True(client.IsConnected);
            Assert.Equal(1, client.ConnectedCount);

            InvokeDisconnected(client, currentConnection, userInitiated: true);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TransportDisconnectCallbackCanReenterDisconnectOrConnectWithoutDeadlock(
            bool reconnect)
        {
            using var handler = new BlockingHttpMessageHandler();
            var configuration = SteamConfiguration.Create(builder => builder
                .WithProtocolTypes(ProtocolTypes.WebSocket)
                .WithHttpClientFactory(_ => new HttpClient(handler, disposeHandler: false)));
            var client = new TestCMClient(configuration);
            using var oldConnection = new CoordinatedDisconnectConnection(
                new DnsEndPoint("old.example.com", 443));
            oldConnection.Disconnected += (_, args) =>
                InvokeDisconnected(client, oldConnection, args.UserInitiated);
            SetConnection(client, oldConnection);
            client.SetIsConnected(true);
            var reentrantCallCount = 0;
            client.DisconnectedAction = () =>
            {
                Interlocked.Increment(ref reentrantCallCount);
                if (reconnect)
                {
                    client.Connect(ServerRecord.CreateWebSocketServer("new.example.com:443"));
                }
                else
                {
                    client.Disconnect();
                }
            };
            var testCancellation = TestContext.Current.CancellationToken;
            Task outerDisconnect = null;
            Task callback = null;

            try
            {
                outerDisconnect = Task.Run(client.Disconnect, testCancellation);
                Assert.True(oldConnection.DisconnectEntered.Wait(
                    TimeSpan.FromSeconds(5),
                    testCancellation));

                callback = Task.Run(
                    () => oldConnection.RaiseDisconnected(userInitiated: true),
                    testCancellation);
                await Task.WhenAll(outerDisconnect, callback)
                    .WaitAsync(TimeSpan.FromSeconds(5), testCancellation);

                Assert.Equal(1, Volatile.Read(ref reentrantCallCount));
                Assert.NotSame(oldConnection, GetConnection(client));
                if (reconnect)
                {
                    Assert.NotNull(GetConnection(client));
                }
                else
                {
                    Assert.Null(GetConnection(client));
                }
            }
            finally
            {
                oldConnection.ForceRelease();
                client.DisconnectedAction = null;
                if (outerDisconnect != null && callback != null)
                {
                    try
                    {
                        await Task.WhenAll(outerDisconnect, callback)
                            .WaitAsync(TimeSpan.FromSeconds(5), testCancellation);
                    }
                    catch
                    {
                    }
                }

                client.Disconnect();
                oldConnection.DisposeTestResources();
            }
        }

        [Fact]
        public async Task OuterConnectReturnsWhenDisconnectCallbackInstallsNewerConnectRequest()
        {
            using var handler = new BlockingHttpMessageHandler();
            var configuration = SteamConfiguration.Create(builder => builder
                .WithProtocolTypes(ProtocolTypes.WebSocket)
                .WithHttpClientFactory(_ => new HttpClient(handler, disposeHandler: false)));
            var client = new TestCMClient(configuration);
            using var oldConnection = new CoordinatedDisconnectConnection(
                new DnsEndPoint("old.example.com", 443));
            oldConnection.Disconnected += (_, args) =>
                InvokeDisconnected(client, oldConnection, args.UserInitiated);
            SetConnection(client, oldConnection);
            client.SetIsConnected(true);
            var reentrantCallCount = 0;
            client.DisconnectedAction = () =>
            {
                Interlocked.Increment(ref reentrantCallCount);
                client.Connect(ServerRecord.CreateWebSocketServer("newer.example.com:443"));
            };
            var testCancellation = TestContext.Current.CancellationToken;
            Task outerConnect = null;
            Task callback = null;

            try
            {
                outerConnect = Task.Run(
                    () => client.Connect(ServerRecord.CreateWebSocketServer("outer.example.com:443")),
                    testCancellation);
                Assert.True(oldConnection.DisconnectEntered.Wait(
                    TimeSpan.FromSeconds(5),
                    testCancellation));

                callback = Task.Run(
                    () => oldConnection.RaiseDisconnected(userInitiated: true),
                    testCancellation);
                await Task.WhenAll(outerConnect, callback)
                    .WaitAsync(TimeSpan.FromSeconds(5), testCancellation);

                Assert.Equal(1, Volatile.Read(ref reentrantCallCount));
                Assert.NotNull(GetConnection(client));
                var endpoint = Assert.IsType<DnsEndPoint>(client.CurrentEndPoint);
                Assert.Equal("newer.example.com", endpoint.Host);
            }
            finally
            {
                oldConnection.ForceRelease();
                client.DisconnectedAction = null;
                if (outerConnect != null && callback != null)
                {
                    try
                    {
                        await Task.WhenAll(outerConnect, callback)
                            .WaitAsync(TimeSpan.FromSeconds(5), testCancellation);
                    }
                    catch
                    {
                    }
                }

                client.Disconnect();
                oldConnection.DisposeTestResources();
            }
        }

        [Fact]
        public async Task SupersededConnectCannotDisconnectTransportInstalledByNewerRequest()
        {
            using var handler = new BlockingHttpMessageHandler();
            var configuration = SteamConfiguration.Create(builder => builder
                .WithProtocolTypes(ProtocolTypes.WebSocket)
                .WithHttpClientFactory(_ => new HttpClient(handler, disposeHandler: false)));
            var client = new TestCMClient(configuration);
            using var oldRequestPaused = new ManualResetEventSlim();
            using var releaseOldRequest = new ManualResetEventSlim();
            var hookCallCount = 0;
            client.ConnectBeforeDisconnectCoreForTesting = () =>
            {
                if (Interlocked.Increment(ref hookCallCount) == 1)
                {
                    oldRequestPaused.Set();
                    releaseOldRequest.Wait();
                }
            };
            var testCancellation = TestContext.Current.CancellationToken;
            Task oldConnect = null;

            try
            {
                oldConnect = Task.Run(
                    () => client.Connect(ServerRecord.CreateWebSocketServer("old-request.example.com:443")),
                    testCancellation);
                Assert.True(oldRequestPaused.Wait(TimeSpan.FromSeconds(5), testCancellation));

                client.Connect(ServerRecord.CreateWebSocketServer("new-request.example.com:443"));
                var newerConnection = GetConnection(client);
                Assert.NotNull(newerConnection);
                var endpointBeforeRelease = Assert.IsType<DnsEndPoint>(client.CurrentEndPoint);
                Assert.Equal("new-request.example.com", endpointBeforeRelease.Host);

                releaseOldRequest.Set();
                await oldConnect.WaitAsync(TimeSpan.FromSeconds(5), testCancellation);

                Assert.Same(newerConnection, GetConnection(client));
                var endpointAfterRelease = Assert.IsType<DnsEndPoint>(client.CurrentEndPoint);
                Assert.Equal("new-request.example.com", endpointAfterRelease.Host);
            }
            finally
            {
                releaseOldRequest.Set();
                client.ConnectBeforeDisconnectCoreForTesting = null;
                if (oldConnect != null)
                {
                    try
                    {
                        await oldConnect.WaitAsync(TimeSpan.FromSeconds(5), testCancellation);
                    }
                    catch
                    {
                    }
                }

                client.Disconnect();
            }
        }

        static byte[] Serialize(ISteamSerializableHeader hdr)
        {
            using var ms = new MemoryStream();
            hdr.Serialize( ms );
            return ms.ToArray();
        }

        static void SetConnection(CMClient client, IConnection connection)
            => typeof(CMClient)
                .GetField("connection", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(client, connection);

        static IConnection GetConnection(CMClient client)
            => (IConnection)typeof(CMClient)
                .GetField("connection", BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(client);

        static void InvokeDisconnected(CMClient client, IConnection sender, bool userInitiated)
            => typeof(CMClient)
                .GetMethod("Disconnected", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(client, new object[] { sender, new DisconnectedEventArgs(userInitiated) });

        static void InvokeConnected(CMClient client, IConnection sender)
            => typeof(CMClient)
                .GetMethod("Connected", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(client, new object[] { sender, EventArgs.Empty });

        sealed class TestCMClient : CMClient
        {
            public TestCMClient()
                : this(SteamConfiguration.CreateDefault())
            {
            }

            public TestCMClient(SteamConfiguration configuration)
                : base(configuration, "CMClientFacts")
            {
            }

            public int DisconnectedCount { get; private set; }
            public int ConnectedCount { get; private set; }
            public Action DisconnectedAction { get; set; }

            protected override void OnClientConnected()
                => ConnectedCount++;

            protected override void OnClientDisconnected(bool userInitiated)
            {
                DisconnectedCount++;
                DisconnectedAction?.Invoke();
            }
        }

        sealed class TestConnection : IConnection, IDisposable
        {
            public TestConnection(EndPoint endpoint)
            {
                CurrentEndPoint = endpoint;
            }

            public event EventHandler<NetMsgEventArgs> NetMsgReceived
            {
                add { }
                remove { }
            }

            public event EventHandler Connected
            {
                add { }
                remove { }
            }

            public event EventHandler<DisconnectedEventArgs> Disconnected;

            public EndPoint CurrentEndPoint { get; }
            public ProtocolTypes ProtocolTypes => ProtocolTypes.WebSocket;
            public int DisposeCount { get; private set; }

            public void Connect(EndPoint endPoint, int timeout = 5000)
                => throw new NotSupportedException();

            public void Disconnect(bool userInitiated)
                => Disconnected?.Invoke(this, new DisconnectedEventArgs(userInitiated));

            public IPAddress GetLocalIP() => IPAddress.None;

            public void Send(Memory<byte> data)
                => throw new NotSupportedException();

            public void Dispose()
                => DisposeCount++;
        }

        sealed class CoordinatedDisconnectConnection : IConnection, IDisposable
        {
            readonly TaskCompletionSource callbackCompleted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            readonly TaskCompletionSource forcedRelease = new(
                TaskCreationOptions.RunContinuationsAsynchronously);

            public CoordinatedDisconnectConnection(EndPoint endpoint)
            {
                CurrentEndPoint = endpoint;
            }

            public event EventHandler<NetMsgEventArgs> NetMsgReceived
            {
                add { }
                remove { }
            }

            public event EventHandler Connected
            {
                add { }
                remove { }
            }

            public event EventHandler<DisconnectedEventArgs> Disconnected;

            public ManualResetEventSlim DisconnectEntered { get; } = new();
            public EndPoint CurrentEndPoint { get; }
            public ProtocolTypes ProtocolTypes => ProtocolTypes.WebSocket;

            public void Connect(EndPoint endPoint, int timeout = 5000)
                => throw new NotSupportedException();

            public void Disconnect(bool userInitiated)
            {
                DisconnectEntered.Set();
                Task.WhenAny(callbackCompleted.Task, forcedRelease.Task).GetAwaiter().GetResult();
            }

            public void RaiseDisconnected(bool userInitiated)
            {
                try
                {
                    Disconnected?.Invoke(this, new DisconnectedEventArgs(userInitiated));
                }
                finally
                {
                    callbackCompleted.TrySetResult();
                }
            }

            public void ForceRelease()
                => forcedRelease.TrySetResult();

            public IPAddress GetLocalIP() => IPAddress.None;

            public void Send(Memory<byte> data)
                => throw new NotSupportedException();

            public void Dispose()
            {
            }

            public void DisposeTestResources()
            {
                ForceRelease();
                DisconnectEntered.Dispose();
            }
        }

        sealed class BlockingHttpMessageHandler : HttpMessageHandler
        {
            protected override async Task<HttpResponseMessage> SendAsync(
                HttpRequestMessage request,
                CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                throw new InvalidOperationException("The blocking handler should only complete through cancellation.");
            }
        }
    }
#endif
}
