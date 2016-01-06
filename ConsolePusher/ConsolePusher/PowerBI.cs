using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace ConsolePusher.PowerBI.Service
{
    public enum CallType
    {
        GET,
        POST
    }

    public class PowerBIServiceHelper
    {
        private string _authorityUri = "https://login.windows.net/common/oauth2/authorize";
        private string _resourceUri = "https://analysis.windows.net/powerbi/api";
        private string _redirectUri = "https://login.live.com/oauth20_desktop.srf";
        private string _requestUri = "https://api.powerbi.com/v1.0/myorg";

        private string _clientId = string.Empty;
        private string _token = string.Empty;
        private bool _isAuthenticated = false;

        public bool IsAuthenticated { get { return _isAuthenticated; } }

        public PowerBIServiceHelper(string clientId)
        {
            this._clientId = clientId;
        }

        public bool Authenticate()
        {
            _token = string.Empty;

            AuthenticationContext authContext = new AuthenticationContext(_authorityUri);
            if (!File.Exists("token.txt"))
            {
                AuthenticationResult ar = authContext.AcquireToken(_resourceUri, _clientId, new Uri(_redirectUri));
                _token = ar.AccessToken;
                File.WriteAllText("token.txt", ar.RefreshToken);
            }
            else
            {
                string refreshToken = File.ReadAllText("token.txt");
                AuthenticationResult ar = authContext.AcquireTokenByRefreshToken(refreshToken, _clientId);
                _token = ar.AccessToken;
            }

            return (!string.IsNullOrEmpty(_token));
        }

        public async Task<JObject> GetDatasets()
        {
            return await CallAPI("datasets", CallType.GET);
        }

        public async Task<JObject> CreateDataset(JObject datasetDefinition, bool useFIFO)
        {
            string endpoint = "datasets";
            if (useFIFO) endpoint += "?defaultRetentionPolicy=basicFIFO";

            return await CallAPI(endpoint, CallType.POST, datasetDefinition);
        }

        public async Task<JObject> AddRows(string datasetId, string tableName, JObject rows)
        {
            string endpoint = "datasets/" + datasetId + "/tables/" + tableName + "/rows";

            return await CallAPI(endpoint, CallType.POST, rows);
        }

        private async Task<JObject> CallAPI(string endpoint, CallType callType, JObject payload = null)
        {
            JObject result = null;
            HttpClient client = null;

            try
            {
                client = new HttpClient();

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _token);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                HttpResponseMessage response = null;
                string fullPathEndpoint = Path.Combine(_requestUri, endpoint);
                Console.WriteLine("Calling " + fullPathEndpoint + "...");

                if (callType == CallType.POST)
                {
                    HttpContent httpContent = null;
                    if (payload != null)
                    {
                        httpContent = new StringContent(payload.ToString(), Encoding.UTF8, "application/json");
                        //Console.WriteLine("Call Payload: " + payload.ToString());
                    }

                    response = await client.PostAsync(fullPathEndpoint, httpContent);
                }
                else
                {
                    response = await client.GetAsync(fullPathEndpoint);
                }

                string responseString = await response.Content.ReadAsStringAsync();
                Console.WriteLine("Response Code: " + response.StatusCode.ToString());
                //Console.WriteLine("Call Response: " + responseString);
                if (response.IsSuccessStatusCode)
                {
                    if (!string.IsNullOrEmpty(responseString)) result = JObject.Parse(responseString);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Call Exception: " + ex.Message);
            }
            finally
            {
                if (client != null)
                    client.Dispose();
            }

            return result;
        }
    }
}
