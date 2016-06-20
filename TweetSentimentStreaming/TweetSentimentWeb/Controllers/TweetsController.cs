using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

using System.Threading.Tasks;
using TweetSentimentWeb.Models;

namespace TweetSentimentWeb.Controllers
{
    public class TweetsController : ApiController
    {
        PhoenixReader hbase = new PhoenixReader();

        public async Task<IEnumerable<Tweet>> GetTweetsByQuery(string query)
        {
            return await hbase.QueryTweetsByKeywordAsync(query);
        }
    }
}