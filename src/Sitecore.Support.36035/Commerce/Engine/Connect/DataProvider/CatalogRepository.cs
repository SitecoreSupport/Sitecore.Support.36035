namespace Sitecore.Support.Commerce.Engine.Connect.DataProvider
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Sitecore.Commerce.Engine.Connect.DataProvider.Caching;
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
        private readonly string _Language;
        private string _Environment;
        private static object _MappingLock = new object();

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
            _Language = language;
        }
        private List<JToken> GetCatalogItems()
        {
            MappingEntriesLastUpdatedUtc = DateTime.UtcNow;
            List<JToken> catalogItems = new List<JToken>();

            var skip = 0;
            var totalItemCount = 1;

            Log.Info("Commerce.Connector - Loading the mapping entries", this);
            Log.Info("Commerce.Connector - Attempting to connect to CE", this);

            if (string.IsNullOrEmpty(_Environment))
            {
                _Environment = CommerceEngineConfiguration.Instance.DefaultEnvironment;
            }

            // Load all CatalogItems in batch of 100
            while (skip < totalItemCount)
            {
                var formattedResponse = InvokeHttpClientGet($"GetCatalogItems(environmentName='{_Environment}',skip={skip},take=100)?$expand=CatalogItems($select=Id,SitecoreId,ParentCatalogList,ParentCategoryList,ChildrenCategoryList,ChildrenSellableItemList,ItemVariations)", true, false);

                if (string.IsNullOrEmpty(formattedResponse))
                {
                    Log.Error("Commerce.Connector - There was an error retrieving the mappings from the Commerce Service", this);
                    return new List<JToken>();
                }

                Log.Info($"Commerce.Connector - Processing response from CE. Skipping {skip} items", this);

                var value = JsonConvert.DeserializeObject<JObject>(formattedResponse, JsonSettings);
                catalogItems.AddRange(value["CatalogItems"]);
                totalItemCount = value["TotalItemCount"].Value<int>();
                skip += 100;
            }

            Log.Info($"Commerce.Connector - Total CatalogItems count {totalItemCount}", this);
            return catalogItems;
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
            MappingEntriesLastUpdatedUtc = DateTime.UtcNow;
            List<JToken> catalogItems = GetCatalogItems();

            foreach (var item in catalogItems)
            {
                // Add an entry for each item
                var entityId = item["Id"].Value<string>();
                var sitecoreId = item["SitecoreId"].Value<string>();
                var type = item["@odata.type"].Value<string>();
                var parentCategoryString = item["ParentCategoryList"].Value<string>();
                var parentCatalogString = item["ParentCatalogList"].Value<string>();

                localMappingEntries.Add(sitecoreId, entityId);

                // Check if we have any parent category present on the item
                if (!string.IsNullOrEmpty(parentCategoryString))
                {
                    MapParentCategories(localMappingEntries, localParentIds, catalogItems, item, entityId, type, parentCategoryString);
                }

                // Determine the action based on the item type
                switch (type)
                {
                    case "#Sitecore.Commerce.Plugin.Catalog.Category":
                        // Create an MappingEntry for the category => parent catalog.
                        MapCategoryToCatalog(localParentIds, item);
                        break;
                    case "#Sitecore.Commerce.Plugin.Catalog.SellableItem":
                        // Create an entry for the variation => sellable item
                        MapSellableItems(localMappingEntries, localParentIds, item, entityId, sitecoreId);
                        // Create an entry for the sellableitem => catalog
                        MapSellableItemToCatalog(localMappingEntries, localParentIds, item);
                        break;
                    case "#Sitecore.Commerce.Plugin.Catalog.Catalog":
                        // Create entries for the catalogs folder to catalog
                        MapCatalogs(localParentIds, item);
                        break;
                    default:
                        break;
                }
            }

            ParentIds = new ConcurrentBag<MappingEntry>(localParentIds);
            MappingEntries = new ConcurrentDictionary<string, string>(localMappingEntries);

            Log.Info(string.Format("Commerce.Connector - Loaded the mapping entries - {0} Entries, {1} Parents", ParentIds.Count, MappingEntries.Count), this);
        }

        private void MapCatalogs(ConcurrentBag<MappingEntry> localParentIds, JToken item)
        {
            // Create an entry for the catalog => catalogs folder
            var catalogId = item["SitecoreId"].Value<string>();
            var catalogFolderId = KnownItemIds.CatalogsItem.Guid.ToString();
            var pathId = catalogId;
            var itemEntityId = item["Id"].Value<string>();

            MappingEntry entry = new MappingEntry { PathId = pathId, SitecoreId = catalogId, ParentId = catalogFolderId, EntityId = itemEntityId };
            localParentIds.Add(entry);
        }

        private void MapSellableItems(Dictionary<string, string> localMappingEntries, ConcurrentBag<MappingEntry> localParentIds, JToken item, string entityId, string sitecoreId)
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

                    // Create a mapping between the variant and its parents by combining the variation entity Id and the parent path Id
                    var parents = localParentIds.Where(p => p.EntityId == entityId);
                    foreach (var parent in parents)
                    {
                        var parentPathId = parent.PathId;
                        var pathId = GuidUtils.GetDeterministicGuidString($"{variationEntityId}|{parentPathId}");
                        MappingEntry categoryEntry = new MappingEntry { PathId = pathId, SitecoreId = pathId, ParentId = parentPathId, EntityId = variationEntityId };
                        localParentIds.Add(categoryEntry);
                        localMappingEntries.Add(pathId, variationEntityId);
                    }
                }
            }
        }

        private void MapCategoryToCatalog(ConcurrentBag<MappingEntry> localParentIds, JToken item)
        {
            var parentCatalogString = item["ParentCatalogList"].Value<string>();
            if (!string.IsNullOrEmpty(parentCatalogString))
            {
                var parentCatalogList = parentCatalogString.Split('|');
                foreach (var parentCatalog in parentCatalogList)
                {
                    var itemId = item["SitecoreId"].Value<string>();
                    var itemEntityId = item["Id"].Value<string>();

                    MappingEntry entry = new MappingEntry { PathId = itemId, SitecoreId = itemId, ParentId = parentCatalog, EntityId = itemEntityId };
                    localParentIds.Add(entry);
                }
            }
        }

        private void MapSellableItemToCatalog(Dictionary<string, string> localMappingEntries, ConcurrentBag<MappingEntry> localParentIds, JToken item)
        {
            var sitecoreId = item["SitecoreId"].Value<string>();
            var parentCatalogString = item["ParentCatalogList"].Value<string>();

            // Create an MappingEntry for the sellable item => parent catalog.
            if (!string.IsNullOrEmpty(parentCatalogString))
            {
                var parentCatalogList = parentCatalogString.Split('|');
                foreach (var parentCatalogId in parentCatalogList)
                {
                    var sellableItemEntityId = $"{sitecoreId}|{parentCatalogId}";
                    var sellableItemSitecoreId = GuidUtils.GetDeterministicGuidString(sellableItemEntityId);
                    var itemId = item["SitecoreId"].Value<string>();
                    var itemEntityId = item["Id"].Value<string>();

                    MappingEntry entry = new MappingEntry { PathId = sellableItemSitecoreId, SitecoreId = itemId, ParentId = parentCatalogId, EntityId = itemEntityId };
                    localParentIds.Add(entry);

                    localMappingEntries.Add(sellableItemSitecoreId, itemEntityId);
                }
            }
        }

        private void MapParentCategories(Dictionary<string, string> localMappingEntries, ConcurrentBag<MappingEntry> localParentIds, List<JToken> catalogItems, JToken item, string entityId, string type, string parentCategoryString)
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
                    var itemId = item["SitecoreId"].Value<string>();
                    var parentCategoryId = parentCategoryItem["SitecoreId"].Value<string>();
                    var pathId = GuidUtils.GetDeterministicGuidString(itemId + "|" + parentCategoryId);
                    var itemEntityId = item["Id"].Value<string>();
                    MappingEntry entry = null;

                    // Categories are not using a deterministic path Id since they are catalog specific
                    if (type == "#Sitecore.Commerce.Plugin.Catalog.Category")
                    {
                        entry = new MappingEntry { PathId = itemId, SitecoreId = itemId, ParentId = parentCategoryId, EntityId = itemEntityId };
                    }
                    else
                    {
                        entry = new MappingEntry { PathId = pathId, SitecoreId = itemId, ParentId = parentCategoryId, EntityId = itemEntityId };
                    }

                    localParentIds.Add(entry);
                    localMappingEntries.Add(pathId, entityId);
                }
            }
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
            this._Environment = configuration.DefaultEnvironment;

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
            client.DefaultRequestHeaders.Add("Language", this._Language);
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

                lock (_MappingLock)
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