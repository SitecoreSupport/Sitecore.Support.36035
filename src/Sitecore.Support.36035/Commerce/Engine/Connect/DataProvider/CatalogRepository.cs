namespace Sitecore.Support.Commerce.Engine.Connect.DataProvider
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Sitecore.Commerce.Engine.Connect.DataProvider.Caching;
    using Sitecore.Commerce.Plugin.Catalog;
    using Sitecore.Commerce.Plugin.ManagedLists;
    using Sitecore.Commerce.Plugin.Shops;
    using Sitecore.Commerce.ServiceProxy;
    using Sitecore.Data;
    using Sitecore.Diagnostics;
    using Sitecore.Globalization;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using static Sitecore.Commerce.Engine.Connect.CommerceConstants;
    using Sitecore.Commerce.Engine.Connect;
    using Sitecore.Commerce.Engine.Connect.DataProvider;

    /// <summary>
    /// This class provides catalog data using Http request to the Commerce Engine service
    /// </summary>
    public class CatalogRepository
    {
        private readonly string _language;
        private string Environment;
        private static object mappingLock = new object();

        /// <summary>
        /// Class used for storing mappings
        /// </summary>
        private class MappingEntry
        {
            /// <summary>
            /// Gets or sets the path identifier.
            /// </summary>
            public string PathId { get; set; }
            /// <summary>
            /// Gets or sets the sitecore identifier.
            /// </summary>
            public string SitecoreId { get; set; }
            /// <summary>
            /// Gets or sets the parent identifier.
            /// </summary>
            public string ParentId { get; set; }
            /// <summary>
            /// Gets or sets the entity identifier.
            /// </summary>
            public string EntityId { get; set; }
        }

        /// <summary>
        /// The mapping entries
        /// </summary>
        public static ConcurrentDictionary<string, string> MappingEntries;

        /// <summary>
        /// The time at which the <see cref="MappingEntries"/> collection was last updated in UTC time.
        /// </summary>
        public static DateTime? MappingEntriesLastUpdatedUtc { get; private set; }

        /// <summary>
        /// The parent ids
        /// </summary>
        private static ConcurrentBag<MappingEntry> ParentIds;

        /// <summary>
        /// The json settings
        /// </summary>
        private static readonly JsonSerializerSettings jsonSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All,
            NullValueHandling = NullValueHandling.Ignore,
            MissingMemberHandling = MissingMemberHandling.Ignore
        };

        /// <summary>
        /// The default cache
        /// </summary>
        private static readonly CommerceCache defaultCache = new CommerceCache("Default");

        /// <summary>
        /// Gets the json settings.
        /// </summary>
        /// <value>
        /// The json settings.
        /// </value>
        public static JsonSerializerSettings JsonSettings => jsonSettings;

        /// <summary>
        /// Gets the default cache.
        /// </summary>
        /// <value>
        /// The default cache.
        /// </value>
        public static CommerceCache DefaultCache => defaultCache;

        /// <summary>
        /// Initializes a new instance of the <see cref="CatalogRepository"/> class.
        /// </summary>
        /// <param name="language">The language.</param>
        public CatalogRepository(string language = "en-US")
        {
            _language = language;
        }


        public string InvokeHttpClientGet(string serviceCallUrl, bool useCommerceOps = false, bool raiseException = true)
        {
            var task = Task.Run(() => InvokeHttpClientGetAsync(serviceCallUrl, useCommerceOps, raiseException));
            task.Wait();

            return task.Result;
        }


        internal async Task<string> InvokeHttpClientGetAsync(string serviceCallUrl, bool useCommerceOps = false, bool raiseException = true)
        {
            try
            {
                using (var client = GetClient(useCommerceOps))
                {
                    var response = await client.GetAsync(serviceCallUrl);

                    if (!response.IsSuccessStatusCode)
                    {
                        LogResponseError(response, raiseException);
                        return string.Empty;
                    }

                    var formattedResponse = await response.Content.ReadAsStringAsync();
                    return formattedResponse;
                }
            }
            catch (Exception ex)
            {
                Log.Error(string.Format(CultureInfo.InvariantCulture, "{0}\r\n{1}", serviceCallUrl, ex), this);
                return string.Empty;
            }
        }

        public void LoadMappingEntries()
        {
            var localMappingEntries = new Dictionary<string, string>();
            var localParentIds = new ConcurrentBag<MappingEntry>();
            //var localParentIds = new List<KeyValuePair<string, string>>();

            var skip = 0;
            var totalItemCount = 1;

            Log.Info("Commerce.Connector - Loading the mapping entries", this);

            Log.Info("Commerce.Connector - Attempting to connect to CE", this);

            if (string.IsNullOrEmpty(Environment))
            {
                Environment = CommerceEngineConfiguration.Instance.DefaultEnvironment;
            }

            MappingEntriesLastUpdatedUtc = DateTime.UtcNow;
            List<JToken> catalogItems = new List<JToken>();

            // Load all CatalogItems in batch of 100
            while (skip < totalItemCount)
            {
                var formattedResponse = InvokeHttpClientGet($"GetCatalogItems(environmentName='{Environment}',skip={skip},take=100)?$expand=CatalogItems($select=Id,SitecoreId,ParentCatalogList,ParentCategoryList,ChildrenCategoryList,ChildrenSellableItemList,ItemVariations)", true, false);

                if (string.IsNullOrEmpty(formattedResponse))
                {
                    ParentIds = new ConcurrentBag<MappingEntry>(localParentIds);
                    MappingEntries = new ConcurrentDictionary<string, string>(localMappingEntries);
                    Log.Error("Commerce.Connector - There was an error retrieving the mappings from the Commerce Service", this);
                    return;
                }

                Log.Info($"Commerce.Connector - Processing response from CE. Skipping {skip} items", this);

                var value = JsonConvert.DeserializeObject<JObject>(formattedResponse, JsonSettings);
                catalogItems.AddRange(value["CatalogItems"]);
                totalItemCount = value["TotalItemCount"].Value<int>();

                skip += 100;
            }

            Log.Info($"Commerce.Connector - Total CatalogItems count {totalItemCount}", this);

            foreach (var item in catalogItems)
            {
                // Add an entry for each item
                var entityId = item["Id"].Value<string>();
                var sitecoreId = item["SitecoreId"].Value<string>();
                var type = item["@odata.type"].Value<string>();
                var parentCategoryString = item["ParentCategoryList"].Value<string>();

                localMappingEntries.Add(sitecoreId, entityId);

                if (!string.IsNullOrEmpty(parentCategoryString))
                {
                    // Get the list of parent categories
                    var parentCategoryList = parentCategoryString.Split('|');

                    foreach (var parentCategory in parentCategoryList)
                    {
                        // Get the parent category
                        var parentCategoryItem = catalogItems.FirstOrDefault(c => c["SitecoreId"].Value<string>() == parentCategory);

                        if (parentCategoryItem != null)
                        {
                            // Create a unique MappingEntry for the parent category => category relationships
                            var childCategoryId = item["SitecoreId"].Value<string>();
                            var parentCategoryId = parentCategoryItem["SitecoreId"].Value<string>();
                            var pathId = GuidUtils.GetDeterministicGuidString(childCategoryId + "|" + parentCategoryId);
                            var itemEntityId = item["Id"].Value<string>();

                            MappingEntry entry = new MappingEntry { PathId = pathId, SitecoreId = childCategoryId, ParentId = parentCategoryId, EntityId = itemEntityId };
                            localParentIds.Add(entry);
                            //localParentIds.Add(new KeyValuePair<string, string>(childId, parentId));
                            localMappingEntries.Add(pathId, entityId);
                        }
                    }
                }

                // Create an MappingEntry for the category => parent catalog.
                if (type == "#Sitecore.Commerce.Plugin.Catalog.Category")
                {
                    var parentCatalogString = item["ParentCatalogList"].Value<string>();
                    if (!string.IsNullOrEmpty(parentCatalogString))
                    {
                        var parentCatalogList = item["ParentCatalogList"].Value<string>().Split('|');
                        foreach (var parentCatalog in parentCatalogList)
                        {
                            var itemId = item["SitecoreId"].Value<string>();
                            var itemEntityId = item["Id"].Value<string>();

                            MappingEntry entry = new MappingEntry { PathId = itemId, SitecoreId = itemId, ParentId = parentCatalog, EntityId = itemEntityId };
                            localParentIds.Add(entry);
                            //localParentIds.Add(new KeyValuePair<string, string>(item["SitecoreId"].Value<string>(), parentCatalog));
                        }
                    }
                }
                // Create an entry for the variation => sellable item
                else if (type == "#Sitecore.Commerce.Plugin.Catalog.SellableItem")
                {
                    var itemVariations = item["ItemVariations"].Value<string>();
                    if (!string.IsNullOrEmpty(itemVariations))
                    {
                        var itemVariationList = itemVariations.Split('|');
                        foreach (var itemVariationId in itemVariationList)
                        {
                            var variationEntityId = $"{entityId}|{itemVariationId}";
                            var variationSitecoreId = GuidUtils.GetDeterministicGuidString(variationEntityId);

                            MappingEntry entry = new MappingEntry { PathId = variationSitecoreId, SitecoreId = variationSitecoreId, ParentId = sitecoreId, EntityId = variationEntityId };
                            localParentIds.Add(entry);

                            localMappingEntries.Add(variationSitecoreId, variationEntityId);
                            //localParentIds.Add(new KeyValuePair<string, string>(variationSitecoreId, sitecoreId));
                        }
                    }
                }
                else if (type == "#Sitecore.Commerce.Plugin.Catalog.Catalog")
                {
                    // Create an entry for the catalog => catalogs folder
                    var catalogId = item["SitecoreId"].Value<string>();
                    var catalogFolderId = KnownItemIds.CatalogsItem.Guid.ToString();
                    var pathId = catalogId;
                    var itemEntityId = item["Id"].Value<string>();

                    MappingEntry entry = new MappingEntry { PathId = pathId, SitecoreId = catalogId, ParentId = catalogFolderId, EntityId = itemEntityId };
                    localParentIds.Add(entry);
                }
            }

            ParentIds = new ConcurrentBag<MappingEntry>(localParentIds);
            MappingEntries = new ConcurrentDictionary<string, string>(localMappingEntries);

            Log.Info(string.Format("Commerce.Connector - Loaded the mapping entries - {0} Entries, {1} Parents", ParentIds.Count, MappingEntries.Count), this);
        }

        /// <summary>
        /// Gets the parent identifier.
        /// </summary>
        /// <param name="childId">The child identifier.</param>
        /// <returns>The ID reprenting the parent of the child</returns>
        public ID GetParentId(ID childId)
        {
            CheckMappings();

            var keyString = childId.ToGuid().ToString();
            var result = ParentIds.FirstOrDefault(k => k.PathId == keyString);

            // If the PathId didn't work, lets try the SitecoreId
            if (result == null)
            {
                result = ParentIds.FirstOrDefault(k => k.SitecoreId == keyString);
            }

            if (result != null)
            {
                return ID.Parse(result.ParentId);
            }

            Log.Error($"Commerce.Connector - Failed to GetParentId for Item Id {childId.ToGuid().ToString()}.", this);

            return ID.Null;
        }

        private HttpClient GetClient(bool useCommerceOps = false)
        {
            var configuration = CommerceEngineConfiguration.Instance;
            this.Environment = configuration.DefaultEnvironment;

            HttpClient client = null;

            if (useCommerceOps)
            {
                client = new HttpClient
                {
                    BaseAddress = new Uri(configuration.CommerceOpsServiceUrl)
                };
            }
            else
            {
                client = new HttpClient
                {
                    BaseAddress = new Uri(configuration.ShopsServiceUrl)
                };
            }

            client.DefaultRequestHeaders.Add("ShopName", configuration.DefaultShopName);
            client.DefaultRequestHeaders.Add("Language", this._language);
            client.DefaultRequestHeaders.Add("Currency", configuration.DefaultShopCurrency);
            client.DefaultRequestHeaders.Add("Environment", configuration.DefaultEnvironment);

            var certificateString = configuration.GetCertificate();
            if (certificateString != null)
            {
                client.DefaultRequestHeaders.Add(configuration.CertificateHeaderName, certificateString);
            }

            client.Timeout =
                configuration.CommerceRequestTimeout == 0
                    ? Timeout.InfiniteTimeSpan
                    : new TimeSpan(0, 0, configuration.CommerceRequestTimeout);

            return client;
        }


        private void CheckMappings(DateTime? requiredUpdateTimeUtc = null)
        {
            // Check if the entries need to be reindexed due to a new item being added to the commerce engine.
            bool forceUpdate = requiredUpdateTimeUtc.HasValue &&
                MappingEntriesLastUpdatedUtc.HasValue &&
                (requiredUpdateTimeUtc > MappingEntriesLastUpdatedUtc);

            if (MappingEntries == null || forceUpdate)
            {
                Log.Info("Commerce.Connector - Acquiring mapping lock", this);

                lock (mappingLock)
                {
                    Log.Info("Commerce.Connector - Mapping locked", this);

                    if (MappingEntries == null || forceUpdate)
                        if (MappingEntries == null || forceUpdate)
                        {
                            LoadMappingEntries();
                        }

                    Log.Info("Commerce.Connector - Release mapping lock", this);
                }
            }
        }

        /// <summary>
        /// Logs the response error.
        /// </summary>
        /// <param name="response">The response.</param>
        /// <param name="raiseError">if set to <c>true</c> [raise error].</param>
        private void LogResponseError(HttpResponseMessage response, bool raiseError = false)
        {
            if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                var authenticationException = new Exception(Translate.Text(Sitecore.Commerce.Engine.Connect.Texts.AuthenticationErrorMessage));
                Log.Error(Translate.Text(Sitecore.Commerce.Engine.Connect.Texts.AuthenticationErrorTitle), authenticationException, this);
                if (raiseError)
                {
                    throw (authenticationException);
                }
            }
            else
            {
                var httpException = new Exception(string.Format(CultureInfo.InvariantCulture, Translate.Text(Sitecore.Commerce.Engine.Connect.Texts.HtppClientErrorMessage), response.StatusCode));
                Log.Error(Translate.Text(Sitecore.Commerce.Engine.Connect.Texts.HtppClientErrorTitle), httpException, this);
                if (raiseError)
                {
                    throw (httpException);
                }
            }
        }

    }
}