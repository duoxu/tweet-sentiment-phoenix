using System;
using System.Diagnostics;
using Tweetinvi;
using Tweetinvi.Core.Enum;

namespace TweetSentimentStreaming
{
    class Program
    {
        const string TWITTERAPPACCESSTOKEN = "<Enter Twitter App Access Token>";
        const string TWITTERAPPACCESSTOKENSECRET = "<Enter Twitter Access Token Secret>";
        const string TWITTERAPPAPIKEY = "<Enter Twitter App API Key>";
        const string TWITTERAPPAPISECRET = "<Enter Twitter App API Secret>";

        static void Main(string[] args)
        {
            TwitterCredentials.SetCredentials(TWITTERAPPACCESSTOKEN, TWITTERAPPACCESSTOKENSECRET,TWITTERAPPAPIKEY, TWITTERAPPAPISECRET);

            Stream_FilteredStreamExample();
        }

        private static void Stream_FilteredStreamExample()
        {
            PhoenixWriter hbase = new PhoenixWriter();
            for (;;)
            {
                try
                {
                    var stream = Stream.CreateFilteredStream();
                    stream.FilterTweetsToBeIn(Language.English);
                    var location = Geo.GenerateLocation(-180, -90, 180, 90);
                    stream.AddLocation(location);
                    var tweetCount = 0;
                    var timer = Stopwatch.StartNew();

                    stream.MatchingTweetReceived += (sender, args) =>
                    {
                        var tweet = args.Tweet;

                        // Write Tweets to HBase
                        if (tweet.Coordinates != null)
                        {
                            hbase.WriteTweet(tweet);
                            tweetCount++;
                        }

                        if (timer.ElapsedMilliseconds > 1000)
                        {
                            if (tweet.Coordinates != null)
                            {

                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.WriteLine("\n{0}: {1} {2}", tweet.Id, tweet.Language.ToString(), tweet.Text);
                                Console.ForegroundColor = ConsoleColor.White;
                                Console.WriteLine("\tLocation: {0}, {1}", tweet.Coordinates.Longitude, tweet.Coordinates.Latitude);
                            }

                            timer.Restart();
                            Console.WriteLine("\tTweets/sec: {0}", tweetCount);
                            tweetCount = 0;
                        }
                    };

                    stream.StartStreamMatchingAllConditions();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: {0}", ex.Message);
                }
            }
        }

    }
}