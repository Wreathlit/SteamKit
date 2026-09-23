using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace SteamKit2
{
    partial class WebSocketConnection : IConnection, IDisposable
    {
        public WebSocketConnection(ILogContext log, HttpMessageInvoker invoker)
        {
            this.log = log ?? throw new ArgumentNullException( nameof( log ) );
            this.invoker = invoker ?? throw new ArgumentNullException( nameof( invoker ) );
        }

        readonly ILogContext log;
        readonly HttpMessageInvoker invoker;
        readonly object contextLock = new();

        WebSocketContext? currentContext;
        LogicalDisconnect? logicalDisconnect;
        ConnectedNotification? connectedNotification;

#if DEBUG
        internal Action? ConnectedNotificationWaitStartedForTesting { get; set; }
        internal Action? LogicalDisconnectWaitStartedForTesting { get; set; }
        internal Action? SendFailureObservedForTesting { get; set; }
#endif

        sealed class LogicalDisconnect
        {
            public LogicalDisconnect(WebSocketContext context, bool userInitiated)
            {
                Context = context;
                UserInitiated = userInitiated;
                OwnerThreadId = Environment.CurrentManagedThreadId;
            }

            public WebSocketContext Context { get; }
            public bool UserInitiated { get; }
            public int OwnerThreadId { get; }
            public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        sealed class ConnectedNotification
        {
            public ConnectedNotification(WebSocketContext context)
            {
                Context = context;
                OwnerThreadId = Environment.CurrentManagedThreadId;
            }

            public WebSocketContext Context { get; }
            public int OwnerThreadId { get; }
            public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public event EventHandler<NetMsgEventArgs>? NetMsgReceived;

        public event EventHandler? Connected;

        public event EventHandler<DisconnectedEventArgs>? Disconnected;

        public EndPoint? CurrentEndPoint { get; set; }
        public ProtocolTypes ProtocolTypes => ProtocolTypes.WebSocket;

        public void Connect(EndPoint endPoint, int timeout = 5000)
        {
            WebSocketContext? newContext = new(this, endPoint);
            try
            {
                var loggedReplacement = false;

                while (true)
                {
                    if (!loggedReplacement)
                    {
                        lock (contextLock)
                        {
                            loggedReplacement = currentContext != null
                                || logicalDisconnect != null
                                || connectedNotification != null;
                        }

                        if (loggedReplacement)
                        {
                            log.LogDebug(nameof(WebSocketConnection), "Attempted to connect while already connected. Closing old connection...");
                        }
                    }

                    DisconnectCore(userInitiated: false, specificContext: null);

                    lock (contextLock)
                    {
                        // Another Connect may have installed a context after DisconnectCore returned.
                        // Last caller wins, matching the previous replacement behavior.
                        if (currentContext != null
                            || (logicalDisconnect != null
                                && logicalDisconnect.OwnerThreadId != Environment.CurrentManagedThreadId)
                            || (connectedNotification != null
                                && connectedNotification.OwnerThreadId != Environment.CurrentManagedThreadId))
                        {
                            continue;
                        }

                        var contextToInstall = newContext;
                        currentContext = contextToInstall;
                        CurrentEndPoint = contextToInstall.EndPoint;
                        try
                        {
                            contextToInstall.Start(invoker, TimeSpan.FromMilliseconds(timeout));
                        }
                        catch
                        {
                            if (ReferenceEquals(currentContext, contextToInstall))
                            {
                                currentContext = null;
                                CurrentEndPoint = null;
                            }

                            throw;
                        }

                        // Ownership has transferred to the connection state. The disconnect path
                        // is now solely responsible for disposing this context.
                        newContext = null;
                        return;
                    }
                }
            }
            finally
            {
                newContext?.Dispose();
            }
        }

        public void Disconnect(bool userInitiated)
            => DisconnectCore(userInitiated, specificContext: null);

        public IPAddress GetLocalIP() => IPAddress.None;

        bool TryRaiseConnected(
            WebSocketContext context,
            CancellationToken cancellationToken,
            Uri connectionUri)
        {
            ConnectedNotification notification;
            lock (contextLock)
            {
                if (cancellationToken.IsCancellationRequested
                    || !ReferenceEquals(currentContext, context)
                    || connectedNotification != null)
                {
                    return false;
                }

                notification = new ConnectedNotification(context);
                connectedNotification = notification;
            }

            try
            {
                log.LogDebug(nameof(WebSocketContext), "Connected to {0}", connectionUri);
                Connected?.Invoke(this, EventArgs.Empty);
                return true;
            }
            finally
            {
                lock (contextLock)
                {
                    if (ReferenceEquals(connectedNotification, notification))
                    {
                        connectedNotification = null;
                    }
                }

                notification.Completion.TrySetResult();
            }
        }

        public void Send(Memory<byte> data)
        {
            var context = Volatile.Read(ref currentContext);
            try
            {
                context?.SendAsync(data).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                log.LogDebug(nameof(WebSocketConnection), "Exception while sending data: {0} - {1}", ex.GetType().FullName, ex.Message);
#if DEBUG
                SendFailureObservedForTesting?.Invoke();
#endif
                // Keep disconnect on the synchronous caller: a Connected callback may be
                // sending its first message and owns the notification completion fence.
                DisconnectCore(userInitiated: false, specificContext: context);
            }
        }

        void DisconnectCore(bool userInitiated, WebSocketContext? specificContext)
        {
            while (true)
            {
                LogicalDisconnect? operationToRun = null;
                LogicalDisconnect? operationToWait = null;
                ConnectedNotification? connectedNotificationToWait = null;
                WebSocketContext? staleContext = null;

                lock (contextLock)
                {
                    if (specificContext == null)
                    {
                        if (currentContext != null)
                        {
                            if (ReferenceEquals(connectedNotification?.Context, currentContext)
                                && connectedNotification.OwnerThreadId != Environment.CurrentManagedThreadId)
                            {
                                connectedNotificationToWait = connectedNotification;
                            }
                            else
                            {
                                operationToRun = new LogicalDisconnect(currentContext, userInitiated);
                                currentContext = null;
                                logicalDisconnect = operationToRun;
                            }
                        }
                        else
                        {
                            operationToWait = logicalDisconnect;
                        }
                    }
                    else if (ReferenceEquals(currentContext, specificContext))
                    {
                        if (ReferenceEquals(connectedNotification?.Context, specificContext)
                            && connectedNotification.OwnerThreadId != Environment.CurrentManagedThreadId)
                        {
                            connectedNotificationToWait = connectedNotification;
                        }
                        else
                        {
                            operationToRun = new LogicalDisconnect(specificContext, userInitiated);
                            currentContext = null;
                            logicalDisconnect = operationToRun;
                        }
                    }
                    else if (ReferenceEquals(logicalDisconnect?.Context, specificContext))
                    {
                        // Cancellation caused by the owning Dispose path can re-enter from the
                        // run loop. The owner is already responsible for the single callback.
                        return;
                    }
                    else
                    {
                        // A superseded run loop may finish after a newer context has become
                        // current. Dispose only that stale context and preserve the active one.
                        staleContext = specificContext;
                    }
                }

                if (connectedNotificationToWait != null)
                {
#if DEBUG
                    ConnectedNotificationWaitStartedForTesting?.Invoke();
#endif
                    connectedNotificationToWait.Completion.Task.GetAwaiter().GetResult();
                    continue;
                }

                if (staleContext != null)
                {
                    staleContext.Dispose();
                    return;
                }

                if (operationToRun != null)
                {
                    CompleteLogicalDisconnect(operationToRun);
                    return;
                }

                if (operationToWait != null
                    && operationToWait.OwnerThreadId != Environment.CurrentManagedThreadId)
                {
                    // IConnection requires the logical Disconnected callback to complete before an
                    // explicit Disconnect returns. Never wait while holding contextLock.
#if DEBUG
                    LogicalDisconnectWaitStartedForTesting?.Invoke();
#endif
                    operationToWait.Completion.Task.GetAwaiter().GetResult();
                }

                return;
            }
        }

        void CompleteLogicalDisconnect(LogicalDisconnect operation)
        {
            try
            {
                operation.Context.Dispose();
            }
            finally
            {
                // CMClient owns its connection state through this callback. Even if the
                // transport cleanup fails, the logical disconnect must still converge so
                // callers do not retain a stale connected client indefinitely.
                try
                {
                    Disconnected?.Invoke(this, new DisconnectedEventArgs(operation.UserInitiated));
                }
                finally
                {
                    lock (contextLock)
                    {
                        if (ReferenceEquals(logicalDisconnect, operation))
                        {
                            logicalDisconnect = null;
                        }

                        // A re-entrant event subscriber may already have installed a new context.
                        if (currentContext == null)
                        {
                            CurrentEndPoint = null;
                        }
                    }

                    operation.Completion.TrySetResult();
                }
            }
        }

        public void Dispose()
        {
            invoker.Dispose();
        }
    }
}
