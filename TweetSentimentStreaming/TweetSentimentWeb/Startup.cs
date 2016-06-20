using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(TweetSentimentWeb.Startup))]
namespace TweetSentimentWeb
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
