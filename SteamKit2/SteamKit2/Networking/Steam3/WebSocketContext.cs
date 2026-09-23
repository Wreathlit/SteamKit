using System;
using System.Buffers;
using System.ComponentModel;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SteamKit2
{
    partial class WebSocketConnection : IConnection
    {
        internal class WebSocketContext : IDisposable
        {
            static long nextContextId;
            // A transport write must finish independently of the GC response budget or the
            // lifetime of this connection. The connection timeout covers only ConnectAsync;
            // it cannot cancel a later stalled write through IConnection's synchronous Send.
            internal static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(5);

#if DEBUG
            internal TimeSpan? SendTimeoutForTesting { get; set; }
            internal Func<Memory<byte>, CancellationToken, ValueTask>? SocketSendOverrideForTesting { get; set; }
#endif

            public WebSocketContext(WebSocketConnection connection, EndPoint endPoint)
            {
                this.connection = connection ?? throw new ArgumentNullException( nameof( connection ) );
                EndPoint = endPoint ?? throw new ArgumentNullException( nameof( endPoint ) );

                contextId = Interlocked.Increment( ref nextContextId );
                cts = new CancellationTokenSource();
                socket = new ClientWebSocket();
                connectionUri = ConstructUri(endPoint);
            }

            readonly WebSocketConnection connection;
            readonly CancellationTokenSource cts;
            readonly ClientWebSocket socket;
            readonly Uri connectionUri;
            readonly long contextId;
            Task? runloopTask;
            int disposed;

            public EndPoint EndPoint { get; }

            public void Start(HttpMessageInvoker invoker, TimeSpan connectionTimeout)
            {
                runloopTask = RunCore(invoker, connectionTimeout, cts.Token).IgnoringCancellation(cts.Token);
            }

            async Task RunCore(HttpMessageInvoker invoker, TimeSpan connectionTimeout, CancellationToken cancellationToken)
            {
                using (var timeout = new CancellationTokenSource())
                using (var combinedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token))
                {
                    timeout.CancelAfter(connectionTimeout);

                    try
                    {
                        await socket.ConnectAsync(connectionUri, invoker, combinedCancellation.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (OperationCanceledException) when (timeout.IsCancellationRequested)
                    {
                        connection.log.LogDebug(nameof(WebSocketContext), "Websocket transport ended; operation=connect; reason=timeout; context={0}; endpoint={1}; timeout={2}", contextId, connectionUri, connectionTimeout);
                        connection.DisconnectCore(userInitiated: false, specificContext: this);
                        return;
                    }
                    catch (Exception ex)
                    {
                        LogTransportFailure( "connect", ex );
                        connection.DisconnectCore(userInitiated: false, specificContext: this);
                        return;
                    }
                }

                if (!connection.TryRaiseConnected(this, cancellationToken, connectionUri))
                {
                    return;
                }

                while (!cancellationToken.IsCancellationRequested && socket.State == WebSocketState.Open)
                {
                    byte[]? packet = null;

                    try
                    {
                        packet = await ReadMessageAsync( cancellationToken ).ConfigureAwait( false );
                    }
                    catch ( OperationCanceledException ) when ( cancellationToken.IsCancellationRequested )
                    {
                        return;
                    }
                    catch ( ObjectDisposedException ) when ( cancellationToken.IsCancellationRequested )
                    {
                        return;
                    }
                    catch ( Exception ex )
                    {
                        LogTransportFailure( "read", ex );
                        connection.DisconnectCore( userInitiated: false, specificContext: this );
                        return;
                    }

                    if (packet != null && packet.Length > 0)
                    {
                        connection.NetMsgReceived?.Invoke(connection, new NetMsgEventArgs(packet, EndPoint));
                    }
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    connection.log.LogDebug(
                        nameof( WebSocketContext ),
                        "Websocket transport ended; operation=read; reason=socket_state; context={0}; endpoint={1}; state={2}",
                        contextId,
                        connectionUri,
                        socket.State );
                    connection.DisconnectCore( userInitiated: false, specificContext: this );
                }
            }

            public async Task SendAsync(Memory<byte> data)
            {
                var sendTimeout = SendTimeout;
#if DEBUG
                sendTimeout = SendTimeoutForTesting ?? sendTimeout;
#endif
                using var deadline = new CancellationTokenSource(sendTimeout);
                try
                {
                    using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, deadline.Token);
                    await SendSocketAsync(data, cancellation.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (deadline.IsCancellationRequested)
                {
                    connection.log.LogDebug(nameof(WebSocketContext),
                        "Websocket transport ended; operation=send; reason=timeout; context={0}; endpoint={1}; timeout={2}",
                        contextId, connectionUri, sendTimeout);
                    // Send may be called synchronously by the Connected notification owner.
                    // Disconnecting on this continuation would wait for that owner while it
                    // waits for us. Let synchronous Send disconnect this exact context instead.
                    throw;
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    return;
                }
                catch (ObjectDisposedException) when (cts.IsCancellationRequested)
                {
                    return;
                }
                catch (WebSocketException ex)
                {
                    LogTransportFailure( "send", ex );
                    throw;
                }
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) == 1)
                {
                    return;
                }

                try
                {
                    cts.Cancel();
                }
                finally
                {
                    runloopTask = null;
                    try
                    {
                        cts.Dispose();
                    }
                    finally
                    {
                        // CloseAsync returns an already-started task, so RunSynchronously cannot
                        // be used here. Disposal is the synchronous disconnect boundary; cancel
                        // the run loop above and always release the socket without waiting for a
                        // graceful close handshake.
                        socket.Dispose();
                    }
                }
            }

            async Task<byte[]?> ReadMessageAsync( CancellationToken cancellationToken )
            {
                var outputBuffer = ArrayPool<byte>.Shared.Rent( 1024 );
                var readBuffer = ArrayPool<byte>.Shared.Rent( 1024 );
                var readMemory = readBuffer.AsMemory();

                ValueWebSocketReceiveResult result;
                var outputLength = 0;

                try
                {
                    do
                    {
                        result = await socket.ReceiveAsync( readMemory, cancellationToken ).ConfigureAwait( false );

                        switch ( result.MessageType )
                        {
                            case WebSocketMessageType.Binary:
                                if ( outputLength + result.Count > outputBuffer.Length )
                                {
                                    var newBuffer = ArrayPool<byte>.Shared.Rent( outputBuffer.Length * 2 );
                                    Buffer.BlockCopy( outputBuffer, 0, newBuffer, 0, outputLength );
                                    ArrayPool<byte>.Shared.Return( outputBuffer );
                                    outputBuffer = newBuffer;
                                }

                                Buffer.BlockCopy( readBuffer, 0, outputBuffer, outputLength, result.Count );
                                outputLength += result.Count;

                                break;

                            case WebSocketMessageType.Text:
                                try
                                {
                                    var message = Encoding.UTF8.GetString( readBuffer, 0, result.Count );
                                    connection.log.LogDebug( nameof( WebSocketContext ), "Received websocket text message: \"{0}\"", message );
                                }
                                catch
                                {
                                    var frameBytes = new byte[ result.Count ];
                                    Array.Copy( readBuffer, 0, frameBytes, 0, result.Count );
                                    connection.log.LogDebug( nameof( WebSocketContext ), "Received websocket text message: 0x{0}", Utils.EncodeHexString( frameBytes ) );
                                }
                                break;

                            case WebSocketMessageType.Close:
                            default:
                                connection.log.LogDebug(
                                    nameof( WebSocketContext ),
                                    "Websocket transport ended; operation=read; reason=remote_close; context={0}; endpoint={1}; close_status={2}; state={3}",
                                    contextId,
                                    connectionUri,
                                    socket.CloseStatus?.ToString() ?? "none",
                                    socket.State );
                                connection.DisconnectCore( userInitiated: false, specificContext: this );
                                return null;
                        }
                    }
                    while ( !result.EndOfMessage );

                    var output = new byte[ outputLength ];
                    Buffer.BlockCopy( outputBuffer, 0, output, 0, output.Length );

                    return output;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return( readBuffer );
                    ArrayPool<byte>.Shared.Return( outputBuffer );
                }
            }

            ValueTask SendSocketAsync(Memory<byte> data, CancellationToken cancellationToken)
            {
#if DEBUG
                if (SocketSendOverrideForTesting is { } send)
                    return send(data, cancellationToken);
#endif
                return socket.SendAsync(data, WebSocketMessageType.Binary, true, cancellationToken);
            }

            void LogTransportFailure( string operation, Exception exception )
            {
                var websocketError = exception is WebSocketException websocketException
                    ? websocketException.WebSocketErrorCode.ToString()
                    : "none";
                var nativeError = exception switch
                {
                    WebSocketException nativeWebsocketException => nativeWebsocketException.NativeErrorCode,
                    Win32Exception win32Exception => win32Exception.NativeErrorCode,
                    _ => 0,
                };
                connection.log.LogDebug(
                    nameof( WebSocketContext ),
                    "Websocket transport ended; operation={0}; reason=exception; context={1}; endpoint={2}; state={3}; exception={4}; websocket_error={5}; native_error={6}; message={7}",
                    operation,
                    contextId,
                    connectionUri,
                    socket.State,
                    exception.GetType().FullName,
                    websocketError,
                    nativeError,
                    exception.Message );
            }

            internal static Uri ConstructUri(EndPoint endPoint)
            {
                var uri = new UriBuilder();
                uri.Scheme = "wss";
                uri.Path = "/cmsocket/";

                switch (endPoint)
                {
                    case IPEndPoint ipep:
                        uri.Port = ipep.Port;
                        uri.Host = ipep.Address.ToString();
                        break;

                    case DnsEndPoint dns:
                        uri.Host = dns.Host;
                        uri.Port = dns.Port;
                        break;

                    default:
                        throw new InvalidOperationException("Unsupported endpoint type.");
                }

                return uri.Uri;
            }
        }
    }
}
