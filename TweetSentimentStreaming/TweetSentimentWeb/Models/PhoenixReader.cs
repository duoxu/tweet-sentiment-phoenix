using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text;
using PhoenixSharp;
using System.Linq;
using PhoenixSharp.Interfaces;
using Apache.Phoenix;
using pbc = Google.Protobuf.Collections;

namespace TweetSentimentWeb.Models
{
    public class PhoenixReader
    {
        // For reading Tweet sentiment data from HDInsight HBase
        PhoenixClient client;

        // HDinsight HBase cluster and HBase table information
        const string CLUSTERNAME = "<HBaseClusterName>";
        const string HADOOPUSERNAME = "<HBaseClusterHadoopUserName>";
        const string HADOOPUSERPASSWORD = "<HBaseCluserUserPassword>";
        const string PHOENIXTABLENAME = "tweets_by_words";

        // The constructor
        public PhoenixReader()
        {
            ClusterCredentials creds = new ClusterCredentials(
                            new Uri(CLUSTERNAME),
                            HADOOPUSERNAME,
                            HADOOPUSERPASSWORD);
            client = new PhoenixClient(creds);
        }

        // Query Tweets sentiment data from the HBase table asynchronously 
        public async Task<IEnumerable<Tweet>> QueryTweetsByKeywordAsync(string keyword)
        {
            List<Tweet> list = new List<Tweet>();

            // Demonstrate Filtering the data from the past 6 hours the row key
            string timeIndex = (ulong.MaxValue -
                (ulong)DateTime.UtcNow.Subtract(new TimeSpan(6, 0, 0)).ToBinary()).ToString().PadLeft(20);
            string startRow = keyword + "_" + timeIndex;
            string endRow = keyword + "|";

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
                openConnResponse = await client.OpenConnectionRequestAsync(connId, info, options);
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
                await client.ConnectionSyncRequestAsync(connId, connProperties, options);
                // Create the statement
                createStatementResponse = await client.CreateStatementRequestAsync(connId, options);
                // Create the table if it does not exist
                string sql = "select * from " + PHOENIXTABLENAME + " where WORD > '" + startRow + "' and WORD < '" + endRow + "'";
                ExecuteResponse execResponse = await client.PrepareAndExecuteRequestAsync(connId, sql, ulong.MaxValue, createStatementResponse.StatementId, options);
                // TODO: Fetch
                pbc::RepeatedField<Row> rows = execResponse.Results[0].FirstFrame.Rows;
                for (int i = 0; i < rows.Count; i++)
                {
                    Row row = rows[i];
                    var coordinates = row.Value[3].Value[0].StringValue;

                    if (coordinates != string.Empty)
                    {
                        string[] lonlat = coordinates.Split(',');

                        var sentimentField = row.Value[4].Value[0].StringValue;
                        Int32 sentiment = Convert.ToInt32(sentimentField);

                        list.Add(new Tweet
                        {
                            Longtitude = Convert.ToDouble(lonlat[0]),
                            Latitude = Convert.ToDouble(lonlat[1]),
                            Sentiment = sentiment
                        });
                    }
                }
            }
            catch (Exception ex)
            {

            }

            if (createStatementResponse != null)
            {
                await client.CloseStatementRequestAsync(connId, createStatementResponse.StatementId, options);
                createStatementResponse = null;
            }

            if (openConnResponse != null)
            {
                await client.CloseConnectionRequestAsync(connId, options);
                openConnResponse = null;
            }

            return list;
        }

        private string GenerateRandomConnId()
        {
            const string hex_characters = "0123456789abcdefghijklmnopqrstuvwxyz";
            var random = new Random();
            // Generating a random connection ID
            return new string(Enumerable.Repeat(hex_characters, 32).Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }

    public class Tweet
    {
        public string IdStr { get; set; }
        public string Text { get; set; }
        public string Lang { get; set; }
        public double Longtitude { get; set; }
        public double Latitude { get; set; }
        public int Sentiment { get; set; }
    }
}