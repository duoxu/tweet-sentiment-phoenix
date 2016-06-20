using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading;
using Tweetinvi.Core.Interfaces;
using PhoenixSharp;
using PhoenixSharp.Interfaces;
using Apache.Phoenix;
using pbc = Google.Protobuf.Collections;

namespace TweetSentimentStreaming
{
    class PhoenixWriter
    {
        // HDinsight HBase cluster and Phoenix table information
        const string CLUSTERNAME = "https://<Enter Your Cluster Name>.azurehdinsight.net/";
        const string HADOOPUSERNAME = "admin"; //the default name is "admin"
        const string HADOOPUSERPASSWORD = "<Enter the Hadoop User Password>";
        const string PHOENIXTABLENAME = "tweets_by_words";

        // Sentiment dictionary file and the punctuation characters
        const string DICTIONARYFILENAME = @"..\..\dictionary.tsv";
        private static char[] _punctuationChars = new[] {
    ' ', '!', '\"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',   //ascii 23--47
    ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '`', '{', '|', '}', '~' };   //ascii 58--64 + misc.

        // For writting to Phoenix
        PhoenixClient client;

        // a sentiment dictionary for estimate sentiment. It is loaded from a physical file.
        Dictionary<string, DictionaryItem> dictionary;

        // use multithread write
        Thread writerThread;
        Queue<ITweet> queue = new Queue<ITweet>();
        bool threadRunning = true;

        // This function connects to Phoenix, loads the sentiment dictionary, and starts the thread for writting.
        public PhoenixWriter()
        {
            ClusterCredentials credentials = new ClusterCredentials(new Uri(CLUSTERNAME), HADOOPUSERNAME, HADOOPUSERPASSWORD);
            client = new PhoenixClient(credentials);

            // create the Phoenix table if it doesn't exist
            string connId = GenerateRandomConnId();
            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();

            // In gateway mode, PQS requests will be https://<cluster dns name>.azurehdinsight.net/hbasephoenix<N>/
            // Requests sent to hbasephoenix0/ will be forwarded to PQS on workernode0
            options.AlternativeEndpoint = "hbasephoenix0/";
            OpenConnectionResponse openConnResponse = null;
            CreateStatementResponse createStatementResponse = null;
            try
            {
                // Opening connection
                pbc::MapField<string, string> info = new pbc::MapField<string, string>();
                openConnResponse = client.OpenConnectionRequestAsync(connId, info, options).Result;
                // Syncing connection
                ConnectionProperties connProperties = new ConnectionProperties
                {
                    HasAutoCommit = true,
                    AutoCommit = true,
                    HasReadOnly = true,
                    ReadOnly = false,
                    TransactionIsolation = 0,
                    Catalog = "",
                    Schema = "",
                    IsDirty = true
                };
                client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();
                // Create the statement
                createStatementResponse = client.CreateStatementRequestAsync(connId, options).Result;
                // Create the table if it does not exist
                string sql = "CREATE TABLE IF NOT EXISTS " + PHOENIXTABLENAME + " (WORD varchar PRIMARY KEY, ID varchar(255), LANG varchar(255), COOR varchar(255), SENTIMENT varchar(255))";
                client.PrepareAndExecuteRequestAsync(connId, sql, 100, createStatementResponse.StatementId, options).Wait();
                Console.WriteLine("Table \"{0}\" is created.", PHOENIXTABLENAME);
            }
            catch (Exception ex)
            {

            }
            finally
            {
                if (createStatementResponse != null)
                {
                    client.CloseStatementRequestAsync(connId, createStatementResponse.StatementId, options).Wait();
                    createStatementResponse = null;
                }

                if (openConnResponse != null)
                {
                    client.CloseConnectionRequestAsync(connId, options).Wait();
                    openConnResponse = null;
                }
            }

            // Load sentiment dictionary from a file
            LoadDictionary();

            // Start a thread for writting to Phoenix
            writerThread = new Thread(new ThreadStart(WriterThreadFunction));
            writerThread.Start();
        }

        ~PhoenixWriter()
        {
            threadRunning = false;
        }

        // Enqueue the Tweets received
        public void WriteTweet(ITweet tweet)
        {
            lock (queue)
            {
                queue.Enqueue(tweet);
            }
        }

        // Load sentiment dictionary from a file
        private void LoadDictionary()
        {
            List<string> lines = File.ReadAllLines(DICTIONARYFILENAME).ToList();
            var items = lines.Select(line =>
            {
                var fields = line.Split('\t');
                var pos = 0;
                return new DictionaryItem
                {
                    Type = fields[pos++],
                    Length = Convert.ToInt32(fields[pos++]),
                    Word = fields[pos++],
                    Pos = fields[pos++],
                    Stemmed = fields[pos++],
                    Polarity = fields[pos++]
                };
            });

            dictionary = new Dictionary<string, DictionaryItem>();
            foreach (var item in items)
            {
                if (!dictionary.Keys.Contains(item.Word))
                {
                    dictionary.Add(item.Word, item);
                }
            }
        }

        // Calculate sentiment score
        private int CalcSentimentScore(string[] words)
        {
            Int32 total = 0;
            foreach (string word in words)
            {
                if (dictionary.Keys.Contains(word))
                {
                    switch (dictionary[word].Polarity)
                    {
                        case "negative": total -= 1; break;
                        case "positive": total += 1; break;
                    }
                }
            }
            if (total > 0)
            {
                return 1;
            }
            else if (total < 0)
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }

        // Popular a batch of rows to be written into Phoenix
        private void CreateTweetByWordsCells(pbc::RepeatedField<UpdateBatch> updates, ITweet tweet)
        {
            var words = tweet.Text.ToLower().Split(_punctuationChars);
            int sentimentScore = CalcSentimentScore(words);
            var word_pairs = words.Take(words.Length - 1)
                                  .Select((word, idx) => string.Format("{0} {1}", word, words[idx + 1]));
            var all_words = words.Concat(word_pairs).ToList();

            foreach (var word in all_words)
            {
                pbc::RepeatedField<TypedValue> parameterValues = new pbc.RepeatedField<TypedValue>();
                var time_index = (ulong.MaxValue -
                    (ulong)tweet.CreatedAt.ToBinary()).ToString().PadLeft(20) + tweet.IdStr;
                TypedValue WORD = new TypedValue
                {
                    StringValue = word + "_" + time_index,
                    Type = Rep.STRING
                };
                parameterValues.Add(WORD);

                TypedValue ID = new TypedValue
                {
                    StringValue = tweet.IdStr,
                    Type = Rep.STRING
                };
                parameterValues.Add(ID);

                TypedValue LANG = new TypedValue
                {
                    StringValue = tweet.Language.ToString(),
                    Type = Rep.STRING
                };
                parameterValues.Add(LANG);

                TypedValue COOR = new TypedValue
                {
                    StringValue = tweet.Coordinates.Longitude.ToString() + "," +
                                      tweet.Coordinates.Latitude.ToString(),
                    Type = Rep.STRING
                };
                parameterValues.Add(COOR);

                TypedValue SENTIMENT = new TypedValue
                {
                    StringValue = sentimentScore.ToString(),
                    Type = Rep.STRING
                };
                parameterValues.Add(SENTIMENT);
                UpdateBatch batch = new UpdateBatch
                {
                    ParameterValues = parameterValues
                };
                updates.Add(batch);
            }
        }

        // Write a Tweet to Phoenix
        public void WriterThreadFunction()
        {
            while (threadRunning)
            {
                // Write rows in batch
                pbc::RepeatedField<UpdateBatch> updates = new pbc.RepeatedField<UpdateBatch>();
                lock (queue)
                {
                    if (queue.Count > 0)
                    {
                        do
                        {
                            if (updates.Count >= 500 || queue.Count == 0)
                            {
                                string connId = GenerateRandomConnId();
                                RequestOptions options = RequestOptions.GetGatewayDefaultOptions();
                                options.TimeoutMillis = 300000;

                                // In gateway mode, PQS requests will be https://<cluster dns name>.azurehdinsight.net/hbasephoenix<N>/
                                // Requests sent to hbasephoenix0/ will be forwarded to PQS on workernode0
                                options.AlternativeEndpoint = "hbasephoenix" + (updates.Count % 2) + "/";
                                OpenConnectionResponse openConnResponse = null;
                                StatementHandle statementHandle = null;
                                try
                                {
                                    // Opening connection
                                    pbc::MapField<string, string> info = new pbc::MapField<string, string>();
                                    openConnResponse = client.OpenConnectionRequestAsync(connId, info, options).Result;
                                    // Syncing connection
                                    ConnectionProperties connProperties = new ConnectionProperties
                                    {
                                        HasAutoCommit = true,
                                        AutoCommit = true,
                                        HasReadOnly = true,
                                        ReadOnly = false,
                                        TransactionIsolation = 0,
                                        Catalog = "",
                                        Schema = "",
                                        IsDirty = true
                                    };
                                    client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();
                                    string sql = "UPSERT INTO " + PHOENIXTABLENAME + " VALUES (?,?,?,?,?)";
                                    PrepareResponse prepareResponse = client.PrepareRequestAsync(connId, sql, 100, options).Result;
                                    statementHandle = prepareResponse.Statement;
                                    ExecuteBatchResponse execResponse = client.ExecuteBatchRequestAsync(connId, statementHandle.Id, updates, options).Result;
                                    Console.WriteLine("===== {0} rows written =====", updates.Count);
                                    updates.Clear();
                                }
                                catch (Exception ex)
                                {
                                    continue;
                                }
                                finally
                                {
                                    if (statementHandle != null)
                                    {
                                        client.CloseStatementRequestAsync(connId, statementHandle.Id, options);
                                        statementHandle = null;
                                    }
                                    if (openConnResponse != null)
                                    {
                                        client.CloseConnectionRequestAsync(connId, options).Wait();
                                        openConnResponse = null;
                                    }
                                }
                            }
                            if (queue.Count > 0)
                            {
                                var tweet = queue.Dequeue();
                                if (tweet.Coordinates != null)
                                {
                                    CreateTweetByWordsCells(updates, tweet);
                                }
                            }
                        } while (queue.Count > 0 || updates.Count > 0);
                    }
                }
                Thread.Sleep(100);
            }
        }

        private string GenerateRandomConnId()
        {
            const string hex_characters = "0123456789abcdefghijklmnopqrstuvwxyz";
            var random = new Random();
            // Generating a random connection ID
            return new string(Enumerable.Repeat(hex_characters, 32).Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
    public class DictionaryItem
    {
        public string Type { get; set; }
        public int Length { get; set; }
        public string Word { get; set; }
        public string Pos { get; set; }
        public string Stemmed { get; set; }
        public string Polarity { get; set; }
    }
}