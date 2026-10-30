using System;
using System.Reflection;
using SteamKit2;
using Xunit;

namespace Tests
{
    public class SteamUserFacts : HandlerTestBase<SteamUser>
    {
        [Fact]
        public void LogOnPostsLoggedOnCallbackWhenNoConnection()
        {
            Handler.LogOn(new SteamUser.LogOnDetails
            {
                Username = "iamauser",
                Password = "lamepassword"
            });

            var callback = SteamClient.GetCallback( );
            Assert.NotNull( callback );
            Assert.IsType<SteamUser.LoggedOnCallback>( callback );

            var loc = (SteamUser.LoggedOnCallback)callback;
            Assert.Equal( EResult.NoConnection, loc.Result );
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
        public void DisconnectAfterLogOnDoesNotLeavePendingJob()
        {
            SteamClient.SetIsConnected( true );
            Handler.LogOn( new SteamUser.LogOnDetails
            {
                Username = "abc",
                AccessToken = "def",
                LoginID = 1,
            } );
            var onClientDisconnected = typeof( SteamClient ).GetMethod(
                "OnClientDisconnected",
                BindingFlags.Instance | BindingFlags.NonPublic );

            // Steam does not echo a logon job id. LogOn must rely on callbacks,
            // not register an unfinishable AsyncJob.
            Assert.Empty( SteamClient.jobManager.asyncJobs );
            Assert.NotNull( onClientDisconnected );

            // CMClient clears this state before invoking SteamClient.OnClientDisconnected.
            SteamClient.SetIsConnected( false );
            onClientDisconnected.Invoke( SteamClient, new object[] { true } );

            Assert.Empty( SteamClient.jobManager.asyncJobs );
            Assert.IsType<SteamClient.DisconnectedCallback>( SteamClient.GetCallback() );
        }
#endif

        [Fact]
        public void LogOnAnonymousPostsLoggedOnCallbackWhenNoConnection()
        {
            Handler.LogOnAnonymous();

            var callback = SteamClient.GetCallback( );
            Assert.NotNull( callback );
            Assert.IsType<SteamUser.LoggedOnCallback>( callback );

            var loc = (SteamUser.LoggedOnCallback)callback;
            Assert.Equal( EResult.NoConnection, loc.Result );
        }
    }
}
