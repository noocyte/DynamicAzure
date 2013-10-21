using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;

namespace Cyan
{
    [DebuggerDisplay("CyanClient({AccountName})")]
    public class CyanClient
    {
        /// <summary>
        /// Creates a CyanClient parsing an Azure Storage connection string.
        /// </summary>
        /// <param name="connectionString">The connection string to be parsed.</param>
        /// <returns>A CyanClient configured via the <code>connectionString</code>.</returns>
        public static CyanClient FromConnectionString(string connectionString)
        {
            var keyValues = CyanUtilities.ParseConnectionStringKeyValues(connectionString);

            string devStorageValue;
            var devStorage = keyValues.TryGetValue("UseDevelopmentStorage", out devStorageValue)
                && devStorageValue.Equals("true", StringComparison.InvariantCultureIgnoreCase);

            if (devStorage)
            {
                // development storage
                if (keyValues.ContainsKey("DevelopmentStorageProxyUri"))
                    throw new NotSupportedException("Development storage proxy is not supported.");

                if (keyValues.ContainsKey("AccountName") || keyValues.ContainsKey("AccountKey"))
                    throw new ArgumentException("You cannot specify an account name/key for development storage.");

                return new CyanClient(true);
            }

            // real thing
            var protocol = CyanUtilities.ParseConnectionStringProtocol(keyValues);
            if (protocol == CyanUtilities.SupportedProtocols.NotSupported)
                throw new NotSupportedException("The specified protocol is not supported.");

            var useSsl = protocol == CyanUtilities.SupportedProtocols.Https;

            string accountName;
            if (!keyValues.TryGetValue("AccountName", out accountName))
                throw new ArgumentException("No account name found in connection string.", "connectionString");

            string accountKey;
            if (!keyValues.TryGetValue("AccountKey", out accountKey))
                throw new ArgumentException("No account key found in connection string.", "connectionString");

            if (keyValues.ContainsKey("TableEndpoint"))
                throw new NotSupportedException("Custom table endpoint is not supported by Cyan yet.");

            return new CyanClient(accountName, accountKey, useSsl);
        }

        /// <summary>
        /// Creates a Cyan client for Azure Table service (the real thing).
        /// </summary>
        /// <param name="accountName">The Azure storage account name.</param>
        /// <param name="accountSecret">The Azure storage account secret.</param>
        /// <param name="commonRetryPolicy">The retry policy that will be used by default for all operations.</param>
        public CyanClient(string accountName, string accountSecret, bool useSsl = false, CyanRetryPolicy commonRetryPolicy = null)
        {
            restClient = new CyanRest(accountName, accountSecret, useSsl, commonRetryPolicy);
        }

        /// <summary>
        /// Creates a Cyan client for development storage ONLY.
        /// </summary>
        /// <param name="useDevelopmentStorage">Must be set to <code>true</code>.</param>
        public CyanClient(bool useDevelopmentStorage)
        {
            if (!useDevelopmentStorage)
                throw new ArgumentException("Use this constructor for development storage.");

            restClient = new CyanRest(developmentStorageAccount, developmentStorageSecret);
        }

        internal const string developmentStorageAccount = "devstoreaccount1";
        const string developmentStorageSecret = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        internal CyanRest restClient;

        /// <summary>
        /// Returns <code>true</code> if the client is using the development storage.
        /// </summary>
        public bool IsDevelopmentStorage { get { return restClient.IsDevelopmentStorage; } }

        /// <summary>
        /// The name of the account in use.
        /// </summary>
        public string AccountName { get { return restClient.AccountName; } }

        /// <summary>
        /// Returns <code>true</code> if the client is using https.
        /// </summary>
        public bool UseSsl { get { return restClient.UseSsl; } }

        /// <summary>
        /// Creates a reference to a table.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>Returns a CyanTable for performing operations on entities in thespecified table.</returns>
        /// <remarks>This method does not perform any request.</remarks>
        public CyanTable this[string tableName]
        {
            get
            {
                CyanUtilities.ValidateTableName(tableName);

                return new CyanTable(tableName, this);
            }
        }

        /// <summary>
        /// Enumerates existing tables.
        /// </summary>
        /// <param name="disableContinuation">If <code>true</code> disables automatic query continuation.</param>
        /// <returns>Returns an enumeration of table names.</returns>
        public IEnumerable<string> QueryTables(bool disableContinuation = false)
        {
            bool hasContinuation = false;
            string nextTable = null;
            do
            {
                var query = hasContinuation ? string.Format("NextTableName={0}", nextTable) : null;

                var response = restClient.GetRequest("Tables", query);
                response.ThrowIfFailed();

                hasContinuation = response.Headers.TryGetValue("x-ms-continuation-NextTableName", out nextTable);

                var entities = CyanSerializer.DeserializeEntities(response.ResponseBody.Root);
                foreach (var entity in entities)
                    yield return entity.TableName;
            } while (!disableContinuation && hasContinuation);
        }

        /// <summary>
        /// Creates a new table.
        /// </summary>
        /// <param name="table">The name of the table to be created.</param>
        public void CreateTable(string table)
        {
            CreateTableImpl(table, true);
        }

        /// <summary>
        /// Tries to create a new table.
        /// </summary>
        /// <param name="table">The name of the table to be created.</param>
        /// <param name="table">The table that has been created or already existed.</param>
        /// <returns>Returns <code>true</code> if the table was created succesfully,
        /// <code>false</code> if the table already exists.</returns>
        public bool TryCreateTable(string tableName, out CyanTable table)
        {
            var ret = TryCreateTable(tableName);
            table = this[tableName];

            return ret;
        }

        /// <summary>
        /// Tries to create a new table.
        /// </summary>
        /// <param name="table">The name of the table to be created.</param>
        /// <returns>Returns <code>true</code> if the table was created succesfully,
        /// <code>false</code> if the table already exists.</returns>
        public bool TryCreateTable(string table)
        {
            return CreateTableImpl(table, false);
        }

        bool CreateTableImpl(string table, bool throwOnConflict)
        {
            CyanUtilities.ValidateTableName(table);

            var entity = CyanEntity.FromObject(new { TableName = table });

            var document = entity.Serialize();
            var response = restClient.PostRequest("Tables", document.ToString());

            if (!throwOnConflict && response.StatusCode == HttpStatusCode.Conflict)
                return false;

            response.ThrowIfFailed();
            return true;
        }

        /// <summary>
        /// Deletes an existing table.
        /// </summary>
        /// <param name="table">The name of the table to be deleted.</param>
        public void DeleteTable(string table)
        {
            CyanUtilities.ValidateTableName(table);
            var resource = string.Format("Tables('{0}')", table);

            var response = restClient.DeleteRequest(resource, "");
            response.ThrowIfFailed();
        }
    }
}
