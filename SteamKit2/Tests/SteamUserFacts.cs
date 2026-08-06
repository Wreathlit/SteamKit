using System;
using System.Reflection;
using System.Threading.Tasks;
using SteamKit2;
using Xunit;

namespace Tests
{
    public class SteamUserFacts : HandlerTestBase<SteamUser>
    {
        [Fact]
        public void LogOnPostsLoggedOnCallbackWhenNoConnection()
        {
            var asyncJob = Handler.LogOn(new SteamUser.LogOnDetails
            {
                Username = "iamauser",
                Password = "lamepassword"
            });

            var callback = SteamClient.GetCallback( );
            Assert.NotNull( callback );
            Assert.IsType<SteamUser.LoggedOnCallback>( callback );

            var loc = (SteamUser.LoggedOnCallback)callback;
            Assert.Equal( EResult.NoConnection, loc.Result );
            Assert.Equal( asyncJob.JobID, loc.JobID );
        }

        [Fact]
        public void LogOnThrowsExceptionIfDetailsNotProvided()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                Handler.LogOn(null);
            });
        }

        [Fact]
        public void LogOnThrowsExceptionIfUsernameNotProvided_OnlyPassword()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                Handler.LogOn(new SteamUser.LogOnDetails
                {
                    Password = "def"
                });
            });
        }

        [Fact]
        public void LogOnThrowsExceptionIfUsernameNotProvided_OnlyAccessToken()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                Handler.LogOn(new SteamUser.LogOnDetails
                {
                    AccessToken = "def"
                });
            });
        }

        [Fact]
        public void LogOnThrowsExceptionIfPasswordAndAccessTokenNotProvided()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                Handler.LogOn(new SteamUser.LogOnDetails
                {
                    Username = "abc"
                });
            });
        }

        [Fact]
        public void LogOnDoesNotThrowExceptionIfUserNameAndPasswordProvided()
        {
            var ex = Record.Exception(() =>
            {
                Handler.LogOn(new SteamUser.LogOnDetails
                {
                    Username = "abc",
                    Password = "def"
                });
            });

            Assert.Null( ex );
        }

        [Fact]
        public void LogOnDoesNotThrowExceptionIfUserNameAndAccessTokenProvided()
        {
            var ex = Record.Exception(() =>
            {
                Handler.LogOn(new SteamUser.LogOnDetails
                {
                    Username = "abc",
                    AccessToken = "def",
                    ShouldRememberPassword = true,
                });
            });

            Assert.Null( ex );
        }

#if DEBUG
        [Fact]
        public async Task DisconnectWhileLogOnIsPendingCancelsJobWithoutCallbackTypeFailure()
        {
            SteamClient.SetIsConnected( true );
            var logOnJob = Handler.LogOn( new SteamUser.LogOnDetails
            {
                Username = "abc",
                AccessToken = "def",
                LoginID = 1,
            } );
            var logOnTask = logOnJob.ToTask();
            var onClientDisconnected = typeof( SteamClient ).GetMethod(
                "OnClientDisconnected",
                BindingFlags.Instance | BindingFlags.NonPublic );

            Assert.False( logOnTask.IsCompleted );
            Assert.NotNull( onClientDisconnected );

            // CMClient clears this state before invoking SteamClient.OnClientDisconnected.
            SteamClient.SetIsConnected( false );
            onClientDisconnected.Invoke( SteamClient, new object[] { true } );

            Assert.True( logOnTask.IsCanceled );
            Assert.False( SteamClient.jobManager.asyncJobs.ContainsKey( logOnJob ) );
            Assert.IsType<SteamClient.DisconnectedCallback>( SteamClient.GetCallback() );
            await Assert.ThrowsAsync<TaskCanceledException>( async () => await logOnTask );
        }
#endif

        [Fact]
        public void LogOnAnonymousPostsLoggedOnCallbackWhenNoConnection()
        {
            var asyncJob = Handler.LogOnAnonymous();

            var callback = SteamClient.GetCallback( );
            Assert.NotNull( callback );
            Assert.IsType<SteamUser.LoggedOnCallback>( callback );

            var loc = (SteamUser.LoggedOnCallback)callback;
            Assert.Equal( EResult.NoConnection, loc.Result );
            Assert.Equal( asyncJob.JobID, loc.JobID );
        }
    }
}
