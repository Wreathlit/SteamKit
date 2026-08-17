using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace SteamKit2.Discovery
{
    /// <summary>
    /// Currently marked quality of a server. All servers start off as Undetermined.
    /// </summary>
    public enum ServerQuality
    {
        /// <summary>
        /// Known good server. A recent <see cref="ServerQuality.Bad"/> report remains authoritative until
        /// <see cref="SmartCMServerList.BadConnectionMemoryTimeSpan"/> expires.
        /// </summary>
        Good,

        /// <summary>
        /// Known bad server.
        /// </summary>
        Bad
    };

    /// <summary>
    /// Smart list of CM servers.
    /// </summary>
    public class SmartCMServerList
    {
        [DebuggerDisplay( "ServerInfo ({Record.EndPoint}, {Protocol}, Bad: {LastBadConnectionTimeUtc.HasValue})" )]
        class ServerInfo
        {
            public ServerInfo( ServerRecord record, ProtocolTypes protocolType )
            {
                Record = record;
                Protocol = protocolType;
            }

            public ServerRecord Record { get; }
            public ProtocolTypes Protocol { get; }
            public DateTime? LastBadConnectionTimeUtc { get; set; }
        }

        /// <summary>
        /// Initialize SmartCMServerList with a given server list provider
        /// </summary>
        /// <param name="configuration">The Steam configuration to use.</param>
        /// <exception cref="ArgumentNullException">The configuration object is null.</exception>
        public SmartCMServerList( SteamConfiguration configuration )
        {
            this.configuration = configuration ?? throw new ArgumentNullException( nameof( configuration ) );
        }

        /// <summary>
        /// The default fallback Websockets server to attempt connecting to if fetching server list through other means fails.
        /// </summary>
        /// <remarks>
        /// If the default server set here no longer works, please create a pull request to update it.
        /// </remarks>
        public static string DefaultServerWebsocket { get; set; } = "cmp1-sea1.steamserver.net:443";

        /// <summary>
        /// The default fallback TCP/UDP server to attempt connecting to if fetching server list through other means fails.
        /// </summary>
        /// <remarks>
        /// If the default server set here no longer works, please create a pull request to update it.
        /// </remarks>
        public static string DefaultServerNetfilter { get; set; } = "ext1-sea1.steamserver.net:27017";

        readonly SteamConfiguration configuration;

        Task? listTask;

        object listLock = new();
        Collection<ServerInfo> servers = [];
        DateTime serversLastRefresh = DateTime.MinValue;
        DateTime nextRefreshAttempt = DateTime.MinValue;
        readonly Dictionary<ProtocolTypes, ulong> nextCandidateSequences = [];

        private void StartFetchingServers()
        {
            lock ( listLock )
            {
                // While a resolve is in flight, every caller awaits its completion instead
                // of starting another concurrent resolve of the same shared list.
                if ( listTask is { IsCompleted: false } )
                {
                    return;
                }

                if ( servers.Count > 0 )
                {
                    // if the server list has been populated, check if it is still fresh
                    if ( DateTime.UtcNow - serversLastRefresh >= ServerListBeforeRefreshTimeSpan )
                    {
                        // A stale list is refreshed at a controlled cadence. Between attempts
                        // the current list, which may be a fallback list from a previous
                        // resolve, keeps serving candidates.
                        if ( DateTime.UtcNow >= nextRefreshAttempt )
                        {
                            listTask = BeginResolveServerListLocked( forceRefresh: true );
                        }
                        else
                        {
                            listTask = Task.CompletedTask;
                        }
                    }
                    else
                    {
                        // no work needs to be done
                        listTask = Task.CompletedTask;
                    }
                }
                else if ( listTask == null || listTask.IsFaulted || listTask.IsCanceled )
                {
                    listTask = BeginResolveServerListLocked( forceRefresh: false );
                }
            }
        }

        Task BeginResolveServerListLocked( bool forceRefresh )
        {
            // Record the attempt before starting: if the resolve cannot obtain a fresh list
            // and has to fall back, the next automatic refresh may only start after
            // ServerListRefreshRetryTimeSpan elapses rather than once per candidate request.
            nextRefreshAttempt = DateTime.UtcNow + ServerListRefreshRetryTimeSpan;

            return ResolveServerList( forceRefresh );
        }

        private bool WaitForServersFetched()
        {
            StartFetchingServers();

            try
            {
                listTask!.GetAwaiter().GetResult();
                return true;
            }
            catch ( Exception ex )
            {
                DebugWrite( $"Failed to retrieve server list: {ex}" );
            }

            return false;
        }

        private async Task ResolveServerList( bool forceRefresh = false )
        {
            var providerRefreshTime = configuration.ServerListProvider.LastServerListRefresh;
            var alreadyTriedDirectoryFetch = false;

            // If this is the first time server list is being resolved,
            // check if the cache is old enough that requires refreshing from the API first
            if ( !forceRefresh && DateTime.UtcNow - providerRefreshTime >= ServerListBeforeRefreshTimeSpan )
            {
                forceRefresh = true;
            }

            // Server list can only be force refreshed if the API is allowed in the first place
            if ( forceRefresh && configuration.AllowDirectoryFetch )
            {
                DebugWrite( $"Querying {nameof( SteamDirectory )} for a fresh server list" );

                var directoryList = await TryLoadDirectoryListAsync().ConfigureAwait( false );
                alreadyTriedDirectoryFetch = true;

                // Fresh server list has been loaded
                if ( directoryList.Count > 0 )
                {
                    DebugWrite( $"Resolved {directoryList.Count} servers from {nameof( SteamDirectory )}" );
                    ReplaceListCore( directoryList, writeProvider: true, DateTime.UtcNow );
                    return;
                }

                DebugWrite( $"Could not query {nameof( SteamDirectory )}, falling back to provider" );
            }
            else
            {
                DebugWrite( "Resolving server list using the provider" );
            }

            IReadOnlyCollection<ServerRecord> endpointList = await TryFetchProviderListAsync().ConfigureAwait( false );

            // Provider server list is fresh enough and it provided servers
            if ( endpointList.Count > 0 )
            {
                DebugWrite( $"Resolved {endpointList.Count} servers from the provider" );
                ReplaceListCore( endpointList, writeProvider: false, providerRefreshTime );
                return;
            }

            // If API fetch is not allowed, bail out with no servers
            if ( !configuration.AllowDirectoryFetch )
            {
                DebugWrite( $"Server list provider had no entries, and {nameof( SteamConfiguration.AllowDirectoryFetch )} is false" );
                ReplaceListCore( [], writeProvider: false, DateTime.MinValue );
                return;
            }

            // If the force refresh tried to fetch the server list already, do not fetch it again
            if ( !alreadyTriedDirectoryFetch )
            {
                DebugWrite( $"Server list provider had no entries, will query {nameof( SteamDirectory )}" );
                endpointList = await TryLoadDirectoryListAsync().ConfigureAwait( false );

                if ( endpointList.Count > 0 )
                {
                    DebugWrite( $"Resolved {endpointList.Count} servers from {nameof( SteamDirectory )}" );
                    ReplaceListCore( endpointList, writeProvider: true, DateTime.UtcNow );
                    return;
                }
            }

            // This is a last effort to attempt any valid connection to Steam
            DebugWrite( $"Server list provider had no entries, {nameof( SteamDirectory )} failed, falling back to default servers" );

            endpointList =
            [
                ServerRecord.CreateWebSocketServer( DefaultServerWebsocket ),
                ServerRecord.CreateDnsSocketServer( DefaultServerNetfilter ),
            ];

            ReplaceListCore( endpointList, writeProvider: false, DateTime.MinValue );
        }

        async Task<IReadOnlyCollection<ServerRecord>> TryLoadDirectoryListAsync()
        {
            try
            {
                return await SteamDirectory.LoadAsync( configuration ).ConfigureAwait( false );
            }
            catch ( Exception ex )
            {
                // A directory failure must not abort the resolve; the fallback ladder
                // (provider, then default servers) still applies.
                DebugWrite( $"Failed to query {nameof( SteamDirectory )}: {ex}" );
                return [];
            }
        }

        async Task<IReadOnlyCollection<ServerRecord>> TryFetchProviderListAsync()
        {
            try
            {
                var serverList = await configuration.ServerListProvider.FetchServerListAsync().ConfigureAwait( false );
                return serverList.ToList();
            }
            catch ( Exception ex )
            {
                // A provider failure must not abort the resolve; the fallback ladder
                // (directory, then default servers) still applies.
                DebugWrite( $"Failed to fetch the server list from the provider: {ex}" );
                return [];
            }
        }

        /// <summary>
        /// Determines how long the server list cache is used as-is before attempting to refresh from the Steam Directory.
        /// </summary>
        public TimeSpan ServerListBeforeRefreshTimeSpan { get; set; } = TimeSpan.FromDays( 7 );

        /// <summary>
        /// Determines how long to wait before another automatic refresh attempt may start after
        /// a refresh could not obtain a fresh server list, for example when a fallback list is
        /// in use because the Steam Directory was unavailable. Between attempts the current
        /// list keeps serving candidates.
        /// </summary>
        public TimeSpan ServerListRefreshRetryTimeSpan { get; set; } = TimeSpan.FromMinutes( 5 );

        /// <summary>
        /// Determines how long a server's bad connection state is remembered for.
        /// </summary>
        public TimeSpan BadConnectionMemoryTimeSpan { get; set; } = TimeSpan.FromMinutes( 5 );

        /// <summary>
        /// Resets the scores of all servers which has a last bad connection more than <see cref="BadConnectionMemoryTimeSpan"/> ago.
        /// </summary>
        public void ResetOldScores()
        {
            var cutoff = DateTime.UtcNow - BadConnectionMemoryTimeSpan;

            lock ( listLock )
            {
                foreach ( var serverInfo in servers )
                {
                    if ( serverInfo.LastBadConnectionTimeUtc.HasValue && serverInfo.LastBadConnectionTimeUtc.Value < cutoff )
                    {
                        serverInfo.LastBadConnectionTimeUtc = null;
                    }
                }
            }
        }

        /// <summary>
        /// Replace the list with a new list of servers provided to us by the Steam servers.
        /// </summary>
        /// <param name="endpointList">The <see cref="ServerRecord"/>s to use for this <see cref="SmartCMServerList"/>.</param>
        /// <param name="writeProvider">If true, the replaced list will be updated in the server list provider.</param>
        /// <param name="serversTime">The time when the provided server list has been updated.</param>
        public void ReplaceList( IEnumerable<ServerRecord> endpointList, bool writeProvider = true, DateTime? serversTime = null )
        {
            ArgumentNullException.ThrowIfNull( endpointList );

            lock ( listLock )
            {
                // An externally supplied list carries no relation to the current one, so all
                // per-endpoint state and the candidate rotation sequences start over.
                nextCandidateSequences.Clear();

                ReplaceListCore( endpointList, writeProvider, serversTime ?? DateTime.UtcNow, preserveEndpointState: false );
            }
        }

        // Replaces the list from an internal refresh. Unlike the public ReplaceList, an
        // internal refresh must not erase what this process has learned about individual
        // CMs: endpoints that survive the refresh keep their bad connection memory, and the
        // rotation sequences keep distributing shared clients across the new list.
        void ReplaceListCore( IEnumerable<ServerRecord> endpointList, bool writeProvider, DateTime serversTime, bool preserveEndpointState = true )
        {
            lock ( listLock )
            {
                var distinctEndPoints = endpointList.Distinct().ToArray();

                Dictionary<(EndPoint EndPoint, ProtocolTypes Protocol), DateTime>? lastBadConnectionTimes = null;

                if ( preserveEndpointState )
                {
                    lastBadConnectionTimes = [];

                    foreach ( var server in servers )
                    {
                        if ( server.LastBadConnectionTimeUtc is DateTime lastBadConnectionTimeUtc )
                        {
                            lastBadConnectionTimes[ (server.Record.EndPoint, server.Protocol) ] = lastBadConnectionTimeUtc;
                        }
                    }
                }

                serversLastRefresh = serversTime;
                servers.Clear();

                for ( var i = 0; i < distinctEndPoints.Length; i++ )
                {
                    AddCore( distinctEndPoints[ i ] );
                }

                if ( lastBadConnectionTimes is { Count: > 0 } )
                {
                    foreach ( var server in servers )
                    {
                        if ( lastBadConnectionTimes.TryGetValue( (server.Record.EndPoint, server.Protocol), out var lastBadConnectionTimeUtc ) )
                        {
                            server.LastBadConnectionTimeUtc = lastBadConnectionTimeUtc;
                        }
                    }
                }

                if ( writeProvider )
                {
                    configuration.ServerListProvider.UpdateServerListAsync( distinctEndPoints ).GetAwaiter().GetResult();
                }
            }
        }

        void AddCore( ServerRecord endPoint )
        {
            foreach ( var protocolType in endPoint.ProtocolTypes.GetFlags() )
            {
                var info = new ServerInfo( endPoint, protocolType );
                servers.Add( info );
            }
        }

        /// <summary>
        /// Explicitly resets the known state of all servers.
        /// </summary>
        public void ResetBadServers()
        {
            lock ( listLock )
            {
                foreach ( var server in servers )
                {
                    if ( server.LastBadConnectionTimeUtc.HasValue )
                    {
                        server.LastBadConnectionTimeUtc = null;
                    }
                }
            }
        }

        internal bool TryMark( EndPoint endPoint, ProtocolTypes protocolTypes, ServerQuality quality )
        {
            lock ( listLock )
            {
                ServerInfo[] serverInfos;

                if ( quality == ServerQuality.Good )
                {
                    serverInfos = servers.Where( x => x.Record.EndPoint.Equals( endPoint ) && x.Protocol.HasFlagsFast( protocolTypes ) ).ToArray();
                }
                else
                {
                    // If we're marking this server for any failure, mark all endpoints for the host at the same time
                    var host = NetHelpers.ExtractEndpointHost( endPoint );
                    serverInfos = servers.Where( x => x.Record.GetHost().Equals( host, StringComparison.Ordinal ) ).ToArray();
                }

                if ( serverInfos.Length == 0 )
                {
                    return false;
                }

                foreach ( var serverInfo in serverInfos )
                {
                    MarkServerCore( serverInfo, quality );
                }

                return true;
            }
        }

        void MarkServerCore( ServerInfo serverInfo, ServerQuality quality )
        {
            switch ( quality )
            {
                case ServerQuality.Good:
                {
                    // A shared server list can receive a transport-connected callback from an
                    // older concurrent attempt after another client has already reported this CM
                    // as unavailable. Keep the newer failure authoritative for the full cooldown;
                    // ResetOldScores will make the endpoint eligible again afterwards.
                    if ( serverInfo.LastBadConnectionTimeUtc is DateTime lastBadConnectionTimeUtc
                        && DateTime.UtcNow - lastBadConnectionTimeUtc >= BadConnectionMemoryTimeSpan )
                    {
                        serverInfo.LastBadConnectionTimeUtc = null;
                    }
                    break;
                }

                case ServerQuality.Bad:
                {
                    serverInfo.LastBadConnectionTimeUtc = DateTime.UtcNow;
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException( nameof( quality ) );
            }
        }

        /// <summary>
        /// Perform the actual score lookup of the server list and return the candidate
        /// </summary>
        /// <returns>IPEndPoint candidate</returns>
        private ServerRecord? GetNextServerCandidateInternal( ProtocolTypes supportedProtocolTypes )
        {
            lock ( listLock )
            {
                // ResetOldScores takes a lock internally, however
                // locks are re-entrant on the same thread, so this
                // isn't a problem.
                ResetOldScores();

                var compatibleServers = servers
                    .Where( o => o.Protocol.HasFlagsFast( supportedProtocolTypes ) )
                    .GroupBy( static server => server.Record.EndPoint )
                    .Select( static group =>
                        group.FirstOrDefault( static server => !server.LastBadConnectionTimeUtc.HasValue )
                        ?? group.First() )
                    .ToArray();
                if ( compatibleServers.Length == 0 )
                {
                    return null;
                }

                var healthyServers = compatibleServers
                    .Where( static server => !server.LastBadConnectionTimeUtc.HasValue )
                    .ToArray();
                var candidates = healthyServers.Length > 0
                    ? healthyServers
                    : compatibleServers;
                nextCandidateSequences.TryGetValue( supportedProtocolTypes, out var nextCandidateSequence );
                var selectedIndex = (int)( nextCandidateSequence % (ulong)candidates.Length );
                nextCandidateSequences[ supportedProtocolTypes ] = nextCandidateSequence == ulong.MaxValue
                    ? 0
                    : nextCandidateSequence + 1;
                var result = candidates[ selectedIndex ];

                DebugWrite(
                    $"Next server candidate: {result.Record.EndPoint} ({result.Protocol}); " +
                    $"healthy_endpoints={healthyServers.Length}; compatible_endpoints={compatibleServers.Length}; " +
                    $"selection_index={selectedIndex}" );
                return new ServerRecord( result.Record.EndPoint, result.Protocol );
            }
        }

        /// <summary>
        /// Get the next server in the list.
        /// </summary>
        /// <param name="supportedProtocolTypes">The minimum supported <see cref="ProtocolTypes"/> of the server to return.</param>
        /// <returns>An <see cref="System.Net.IPEndPoint"/>, or null if the list is empty.</returns>
        public ServerRecord? GetNextServerCandidate( ProtocolTypes supportedProtocolTypes )
        {
            if ( !WaitForServersFetched() )
            {
                return null;
            }

            return GetNextServerCandidateInternal( supportedProtocolTypes );
        }

        /// <summary>
        /// Get the next server in the list.
        /// </summary>
        /// <param name="supportedProtocolTypes">The minimum supported <see cref="ProtocolTypes"/> of the server to return.</param>
        /// <returns>An <see cref="System.Net.IPEndPoint"/>, or null if the list is empty.</returns>
        public async Task<ServerRecord?> GetNextServerCandidateAsync( ProtocolTypes supportedProtocolTypes )
        {
            StartFetchingServers();
            await listTask!.ConfigureAwait( false );

            return GetNextServerCandidateInternal( supportedProtocolTypes );
        }

        /// <summary>
        /// Gets the <see cref="System.Net.IPEndPoint"/>s of all servers in the server list.
        /// </summary>
        /// <returns>An <see cref="T:System.Net.IPEndPoint[]"/> array contains the <see cref="System.Net.IPEndPoint"/>s of the servers in the list</returns>
        public ServerRecord[] GetAllEndPoints()
        {
            ServerRecord[] endPoints;

            if ( !WaitForServersFetched() )
            {
                return [];
            }

            lock ( listLock )
            {
                endPoints = servers.Select( static s => s.Record ).Distinct().ToArray();
            }

            return endPoints;
        }

        /// <summary>
        /// Force refresh the server list. If directory fetch is allowed, it will refresh from the API first,
        /// and then fallback to the server list provider.
        /// </summary>
        /// <returns>Task to be awaited that refreshes the server list.</returns>
        public Task ForceRefreshServerList()
        {
            lock ( listLock )
            {
                // Join a resolve that is already in flight instead of starting another
                // concurrent resolve of the same shared list.
                if ( listTask is { IsCompleted: false } resolveInFlight )
                {
                    return resolveInFlight;
                }

                listTask = BeginResolveServerListLocked( forceRefresh: true );

                return listTask;
            }
        }

        static void DebugWrite( string msg )
        {
            DebugLog.WriteLine( "ServerList", msg );
        }
    }
}
