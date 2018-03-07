namespace Sitecore.Support.Commerce.Engine.Connect.DataProvider
{
    using Sitecore.Data;
    using Sitecore.Data.DataProviders;

    public class CatalogDataProvider : Sitecore.Commerce.Engine.Connect.DataProvider.CatalogDataProvider
    {
        public override ID GetParentID(ItemDefinition itemDefinition, CallContext context)
        {
            if (CanProcessParent(itemDefinition))
            {
                var repository = new CatalogRepository();
                var result = repository.GetParentId(itemDefinition.ID);

                if (!result.IsNull)
                {
                    context.Abort();
                    return result;
                }
            }

            return null;
        }
    }
}