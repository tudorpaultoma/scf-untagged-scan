/*
  SCF Untagged Resource Scanner

  Purpose:
  - Enumerates resources across multiple Tencent Cloud services and regions
  - Finds resources without tags
  - Optionally exports a CSV summary to COS

  Notes:
  - Read-only API calls (Describe/List/Get) are used for discovery
  - COS export requires write permission (cos:PutObject) to the target bucket/key
  - Environment variables can limit scope and control timeouts for safer execution in SCF
*/
const tencentcloud = require("tencentcloud-sdk-nodejs");
const COS = require("cos-nodejs-sdk-v5");

/* ====== Config ======
   SDK imports and configuration. Credentials can come from:
   - Explicit env vars (TENCENTCLOUD_SECRETID/SECRETKEY/SESSIONTOKEN variants)
   - SCF role via SDK default provider chain when no explicit env creds are present
*/
const { TENCENTCLOUD_REGION = "eu-frankfurt" } = process.env; // Default control-plane region used to discover regions
// Support SCF-provided env names and user-provided variants
const ENV_SECRET_ID =
  process.env.TENCENTCLOUD_SECRETID || process.env.TENCENTCLOUD_SECRET_ID; // Access key ID
const ENV_SECRET_KEY =
  process.env.TENCENTCLOUD_SECRETKEY || process.env.TENCENTCLOUD_SECRET_KEY; // Access key secret
const ENV_TOKEN =
  process.env.TENCENTCLOUD_SESSIONTOKEN || process.env.TENCENTCLOUD_SESSION_TOKEN; // Optional session token (temporary creds)

if (!ENV_SECRET_ID || !ENV_SECRET_KEY) {
  
}

/**
 * Build base SDK client config for a given region.
 * If explicit credentials are provided via env, they are attached.
 * Otherwise, the SDK default provider chain (e.g., SCF role) is used.
 */
const baseConfig = (region) => {
  const cfg = { region };
  if (ENV_SECRET_ID && ENV_SECRET_KEY) {
    cfg.credential = {
      secretId: ENV_SECRET_ID,
      secretKey: ENV_SECRET_KEY,
      token: ENV_TOKEN,
    };
  }
  // If no env creds, rely on SDK default provider chain (SCF role)
  return cfg;
};

/* ====== Execution Controls (via environment) ======
   SCAN_REGIONS            Comma list of regions to scan; if set, avoids DescribeRegions. Example: "eu-frankfurt,ap-singapore"
   SCAN_SERVICES           Comma list of service scanner keys to run. Example: "CVM,SCF,VPC,COS"
   ENABLE_COS_SCAN         "true"/"false" to enable scanning COS buckets for untagged buckets (requires COS read perms)
   MAX_REGION_CONCURRENCY  Max concurrent region workers to balance speed vs. API limits
   SERVICE_TIMEOUT_MS      Per-service overall timeout
   REQUEST_TIMEOUT_MS      Per-page request timeout within paginated fetches
   MAX_PAGES_PER_LIST      Safety cap for pages to prevent unbounded scans
*/
const {
  SCAN_REGIONS,                 // e.g. "eu-frankfurt,ap-singapore"
  SCAN_SERVICES,                // e.g. "CVM,SCF,VPC,COS"
  ENABLE_COS_SCAN = "true",     // "true" | "false"
  MAX_REGION_CONCURRENCY = "3", // e.g. "2"
  SERVICE_TIMEOUT_MS = "20000", // 20s per service call
  REQUEST_TIMEOUT_MS = "15000", // 15s per paged request
  MAX_PAGES_PER_LIST = "50"     // safety cap for pagination
} = process.env;

const parsedMaxRegionConc = Math.max(1, parseInt(MAX_REGION_CONCURRENCY, 10) || 3); // normalized numeric concurrency
const parsedServiceTimeout = Math.max(2000, parseInt(SERVICE_TIMEOUT_MS, 10) || 20000); // min 2s guard
const parsedRequestTimeout = Math.max(2000, parseInt(REQUEST_TIMEOUT_MS, 10) || 15000); // min 2s guard
const parsedMaxPagesPerList = Math.max(1, parseInt(MAX_PAGES_PER_LIST, 10) || 50); // pagination safety cap

/**
 * Wrap a promise with a timeout to prevent long-hanging API calls.
 * @param {Promise} promise - async work to bound
 * @param {number} ms - timeout in milliseconds
 * @param {string} tag - label to include in timeout error
 */
function withTimeout(promise, ms, tag = "op") {
  return Promise.race([
    promise,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout:${tag}`)), ms))
  ]);
}

/* ====== Helpers ======
   Generic utilities for tag extraction and pagination handling.
*/
/**
 * Attempt to extract a tag collection from various possible shapes returned by different services.
 * Returns an array of tags or an array of tag keys depending on source structure.
 */
const extractTags = (obj) => {
  if (!obj) return [];
  const candidates = [
    obj.Tags,
    obj.TagSet,
    obj.TagList,
    obj.ResourceTags,
    obj.Tags?.TagSet,
  ].filter(Boolean);
  for (const c of candidates) {
    if (Array.isArray(c)) return c;
    if (typeof c === "object") return Object.keys(c);
  }
  return [];
};
/**
 * Determine if a resource-like object has no tags (empty or missing).
 */
const hasNoTags = (obj) => {
  const tags = extractTags(obj);
  return !tags || tags.length === 0;
};

/**
 * Paginated fetch with timeout and page cap support.
 * Calls fetchPage({ Offset, Limit }) repeatedly and concatenates results into a flat array.
 */
async function pagedFetch(fetchPage, pageSize = 100, startOffset = 0, offsetKey = "Offset", limitKey = "Limit") {
  let offset = startOffset;
  const all = [];
  let pages = 0;
  while (true) {
    const page = await withTimeout(fetchPage({ [offsetKey]: offset, [limitKey]: pageSize }), parsedRequestTimeout, "page");
    const items = page.items || [];
    all.push(...items);
    pages++;
    if (items.length < pageSize) break;
    if (pages >= parsedMaxPagesPerList) break; // safety cap
    offset += pageSize;
  }
  return all;
}

/**
 * Resolve list of regions to scan.
 * - If SCAN_REGIONS is set, uses that directly (no DescribeRegions call).
 * - Otherwise queries CVM DescribeRegions to discover available regions.
 */
async function listAllRegions() {
  if (SCAN_REGIONS && SCAN_REGIONS.trim()) {
    return SCAN_REGIONS.split(",").map(s => s.trim()).filter(Boolean);
  }
  const CvmClient = tencentcloud.cvm.v20170312.Client;
  const client = new CvmClient({
    ...baseConfig(TENCENTCLOUD_REGION),
    profile: { httpProfile: { endpoint: "cvm.tencentcloudapi.com" } },
  });
  const res = await withTimeout(client.DescribeRegions({}), parsedRequestTimeout, "DescribeRegions");
  const regs = (res.RegionSet || []).map((r) => r.Region).filter(Boolean);
  return Array.from(new Set(regs));
}

/**
 * NOTE: This redeclares pagedFetch and overrides the earlier timeout-aware version above.
 * It retains simple pagination without per-request timeout or page cap.
 * Kept for backward compatibility; do not change behavior here.
 */
// General pagination helper
async function pagedFetch(fetchPage, pageSize = 100, startOffset = 0, offsetKey = "Offset", limitKey = "Limit") {
  let offset = startOffset;
  const all = [];
  while (true) {
    const page = await fetchPage({ [offsetKey]: offset, [limitKey]: pageSize });
    const items = page.items || [];
    all.push(...items);
    if (items.length < pageSize) break;
    offset += pageSize;
  }
  return all;
}

/* ====== Region-scoped scanners ======
   Factory that creates per-region service scanners.
   Each method returns a list of items with shape: { service, id, [region] }
   Only read-only Describe/List/Get APIs are used.
*/
function scannersForRegion(region) {
  const clients = {};
  /**
   * Lazily create and cache a typed client for a given service/version/endpoint in this region.
   */
  const getClient = (svc, version, endpoint) => {
    const key = `${svc}-${version}`;
    if (!clients[key]) {
      const ClientCtor = tencentcloud[svc][version].Client;
      clients[key] = new ClientCtor({
        ...baseConfig(region),
        profile: { httpProfile: { endpoint } },
      });
    }
    return clients[key];
  };

  return {
    // CVM instances (compute)
    async CVM() {
      const client = getClient("cvm", "v20170312", "cvm.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CVM", id: x.InstanceId }));
    },
    // CBS disks (block storage)
    async CBS() {
      const client = getClient("cbs", "v20170312", "cbs.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDisks({ Offset, Limit });
        return { items: res.DiskSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CBS", id: x.DiskId }));
    },
    // CLB load balancers
    async CLB() {
      const client = getClient("clb", "v20180317", "clb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeLoadBalancers({ Offset, Limit });
        return { items: res.LoadBalancerSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CLB", id: x.LoadBalancerId }));
    },
    // SCF functions (serverless)
    async SCF() {
      const client = getClient("scf", "v20180416", "scf.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.ListFunctions({ Offset, Limit });
        return { items: res.Functions || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "SCF", id: x.FunctionName }));
    },
    // TKE clusters (standard + serverless)
    async TKE() {
      const client = getClient("tke", "v20180525", "tke.tencentcloudapi.com");

      // Standard clusters
      const standardClusters = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeClusters({ Offset, Limit });
        return { items: res.Clusters || res.ClusterSet || [] };
      });

      // Serverless (EKS) clusters — note the correct API name casing: DescribeEKSClusters
      const serverlessClusters = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeEKSClusters({ Offset, Limit });
        return { items: res.Clusters || res.ClusterSet || res.EksClusterSet || res.ClusterInfos || [] };
      });

      const untaggedStandard = standardClusters
        .filter(hasNoTags)
        .map((x) => ({ service: "TKE", id: x.ClusterId }));

      const untaggedServerless = serverlessClusters
        .filter(hasNoTags)
        .map((x) => ({ service: "TKE_SERVERLESS", id: x.ClusterId || x.EksClusterId || x.ClusterName }));

      return [...untaggedStandard, ...untaggedServerless];
    },
    // TCR registries
    async TCR() {
      const client = getClient("tcr", "v20190924", "tcr.tencentcloudapi.com");
      const res = await client.DescribeInstances({});
      const items = res.Registries || res.Instances || [];
      return items.filter(hasNoTags).map((x) => ({ service: "TCR", id: x.RegistryId || x.InstanceId }));
    },
    // VPC bandwidth packages
    async BANDWIDTH_PACK() {
      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeBandwidthPackages({ Offset, Limit });
        return { items: res.BandwidthPackageSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "BANDWIDTH_PACK", id: x.BandwidthPackageId }));
    },
    // VPC VPN gateways
    async VPN() {
      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeVpnGateways({ Offset, Limit });
        return { items: res.VpnGatewaySet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "VPN", id: x.VpnGatewayId }));
    },
    // VPC CCN (global) - only emitted once from the designated home region
    async CCN() {
      // CCN is global; emit once and mark region as "global"
      if (region !== "eu-frankfurt") return [];

      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeCcns({ Offset, Limit });
        return { items: res.CcnSet || res.Ccns || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CCN", id: x.CcnId, region: "global" }));
    },
    // VPC NAT gateways
    async NAT_GATEWAY() {
      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeNatGateways({ Offset, Limit });
        return { items: res.NatGatewaySet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "NAT_GATEWAY", id: x.NatGatewayId }));
    },
    // VPC Elastic IPs (only unbound and untagged)
    async EIP() {
      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeAddresses({ Offset, Limit });
        return { items: res.AddressSet || [] };
      });
      const isUnbound = (x) => {
        const status = String(x.AddressStatus || x.Status || "").toUpperCase();
        const boundLike = new Set(["BIND", "BINDING", "ASSOCIATED", "ASSOCIATING"]);
        const unboundLike = new Set(["UNBIND", "UNBINDING", "UNBOUND", "AVAILABLE"]);
        if (boundLike.has(status)) return false;
        if (unboundLike.has(status)) return true;
        // Fallback: consider bound if any association fields are present
        return !(x.InstanceId || x.NetworkInterfaceId || x.PrivateAddressId || x.AddressBindInfo);
      };
      return items
        .filter((x) => hasNoTags(x) && isUnbound(x))
        .map((x) => ({ service: "EIP", id: x.AddressId || x.AddressIp || x.PublicIp }));
    },
    // Lighthouse instances (lightweight compute)
    async LIGHTHOUSE() {
      const client = getClient("lighthouse", "v20200324", "lighthouse.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "LIGHTHOUSE", id: x.InstanceId }));
    },
    // CLS logsets
    async CLS() {
      const client = getClient("cls", "v20201016", "cls.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeLogsets({ Offset, Limit });
        return { items: res.Logsets || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CLS", id: x.LogsetId }));
    },
    // Anti-DDoS advanced instances
    async ANTIDDOS() {
      const client = getClient("antiddos", "v20200309", "antiddos.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeListBGPInstances({ Offset, Limit });
        return { items: res.Data?.List || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "ANTIDDOS", id: x.InstanceId }));
    },
    // TDMQ for CKafka instances
    async TDMQ_CKAFKA() {
      const client = getClient("ckafka", "v20190819", "ckafka.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceList || res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_CKAFKA", id: x.InstanceId }));
    },
    // TDMQ for RocketMQ clusters
    async TDMQ_ROCKETMQ() {
      const client = getClient("tdmq", "v20200217", "tdmq.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeRocketMQClusters({ Offset, Limit });
        return { items: res.ClusterInfoList || res.ClusterList || res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_ROCKETMQ", id: x.ClusterId || x.InstanceId }));
    },
    // TDMQ for RabbitMQ serverless instances
    async TDMQ_RABBITMQ() {
      const client = getClient("tdmq", "v20200217", "tdmq.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeRabbitMQServerlessInstances({ Offset, Limit });
        return { items: res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_RABBITMQ", id: x.InstanceId || x.ClusterId }));
    },
    // TDMQ for Pulsar Pro instances
    async TDMQ_PULSAR() {
      const client = getClient("tdmq", "v20200217", "tdmq.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribePulsarProInstances({ Offset, Limit });
        return { items: res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_PULSAR", id: x.InstanceId }));
    },
    // Cloud Firewall (CWF) NAT firewall instances
    async CLOUD_FIREWALL() {
      try {
        const client = getClient("cfw", "v20190904", "cfw.tencentcloudapi.com");
        const res = await client.DescribeNatFirewallInstancesInfo({ Limit: 100 });
        const items = res.Data?.NatFirewallInfo || [];
        return items.filter(hasNoTags).map((x) => ({ service: "CLOUD_FIREWALL", id: x.NatInsId || x.InstanceId }));
      } catch {
        return [];
      }
    },
    // TencentDB for MySQL instances
    async MYSQL() {
      const client = getClient("cdb", "v20170320", "cdb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.Items || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "MYSQL", id: x.InstanceId }));
    },
    // TencentDB for SQL Server instances
    async MSSQL() {
      const client = getClient("sqlserver", "v20180328", "sqlserver.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.DBInstances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "MSSQL", id: x.InstanceId }));
    },
    // TencentDB for PostgreSQL instances
    async POSTGRES() {
      const client = getClient("postgres", "v20170312", "postgres.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.DBInstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "POSTGRES", id: x.DBInstanceId }));
    },
    // TDSQL (DCDB) instances
    async TDSQL() {
      const client = getClient("dcdb", "v20180411", "dcdb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDCDBInstances({ Offset, Limit });
        return { items: res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDSQL", id: x.InstanceId }));
    },
    // TDSQL-C (CynosDB) clusters
    async CYNOSDB() {
      const client = getClient("cynosdb", "v20190107", "cynosdb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeClusters({ Offset, Limit });
        return { items: res.ClusterSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDSQL_C", id: x.ClusterId }));
    },
    // Redis instances
    async REDIS() {
      const client = getClient("redis", "v20180412", "redis.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "REDIS", id: x.InstanceId }));
    },
    // MongoDB instances
    async MONGODB() {
      const client = getClient("mongodb", "v20190725", "mongodb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.InstanceDetails || res.InstanceList || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "MONGODB", id: x.InstanceId || x.InstanceName }));
    },
    // TEM applications
    async TEM() {
      const client = getClient("tem", "v20210701", "tem.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeApplications({ Offset, Limit });
        return { items: res.Result?.Applications || res.Applications || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TEM", id: x.ApplicationId || x.Id || x.ApplicationName }));
    },
    // Private DNS zones (global) - only emitted once from the designated home region
    async PRIVATE_DNS() {
      // Private DNS is global; emit once and mark region as "global"
      if (region !== "eu-frankfurt") return [];

      const client = getClient("privatedns", "v20201028", "privatedns.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribePrivateZoneList({ Offset, Limit });
        return { items: res.PrivateZoneSet || res.PrivateZones || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "PRIVATE_DNS", id: x.ZoneId || x.ZoneName, region: "global" }));
    },
    // ADP applications
    async ADP() {
      const client = getClient("adp", "v20220101", "adp.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeApplications({ Offset, Limit });
        return { items: res.Applications || res.Apps || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "ADP", id: x.AppId || x.ApplicationId || x.Name }));
    },
    // Live (CSS) domains
    async CSS_DOMAINS() {
      const client = getClient("live", "v20180801", "live.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeLiveDomains({ Offset, Limit });
        return { items: res.Domains || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CSS_DOMAINS", id: x.DomainName }));
    },
    // GAAP proxy groups
    async GAAP_GROUP() {
      const client = getClient("gaap", "v20180529", "gaap.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeProxyGroupList({ Offset, Limit });
        return { items: res.ProxyGroupList || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "GAAP_GROUP", id: x.GroupId || x.ProxyGroupId }));
    },
    // CTSDB instances
    async CTSDB() {
      const client = getClient("ctsdb", "v20190401", "ctsdb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CTSDB", id: x.InstanceId }));
    },
    // Tendis instances
    async TENDIS() {
      const client = getClient("tendis", "v20190708", "tendis.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeTendisInstances({ Offset, Limit });
        return { items: res.InstanceSet || res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TENDIS", id: x.InstanceId }));
    },
    // VectorDB instances
    async VECTORDB() {
      const client = getClient("vectordb", "v20240223", "vectordb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances?.({ Offset, Limit }) || {};
        return { items: res.DBInstances || res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "VECTORDB", id: x.InstanceId || x.Id }));
    },
    // DLC data engines
    async DLC() {
      const client = getClient("dlc", "v20210125", "dlc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDataEngines({ Offset, Limit });
        return { items: res.DataEngines || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "DLC", id: x.DataEngineId || x.DataEngineName }));
    },
    // ClickHouse (cdwch) instances
    async TCHOUSE_C() {
      const client = getClient("cdwch", "v20200915", "cdwch.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.Instances || res.InstancesList || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TCHOUSE_C", id: x.InstanceId }));
    },
    // ClickHouse (cdwpg) instances (managed)
    async TCHOUSE_P() {
      const client = getClient("cdwpg", "v20181225", "cdwpg.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceList || res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TCHOUSE_P", id: x.InstanceId }));
    },
    // Doris (cdwdoris) instances
    async TCHOUSE_D() {
      const client = getClient("cdwdoris", "v20211228", "cdwdoris.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.Instances || res.InstancesList || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TCHOUSE_D", id: x.InstanceId }));
    },
    // KMS keys (metadata only)
    async KMS_KEYS() {
      const client = getClient("kms", "v20190118", "kms.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.ListKeys({ Offset, Limit });
        return { items: res.Keys || res.KeyMetadatas || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "KMS_KEYS", id: x.KeyId }));
    },
    // SSM secrets (metadata only)
    async SSM_SECRETS() {
      const client = getClient("ssm", "v20190923", "ssm.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.ListSecrets({ Offset, Limit });
        return { items: res.SecretMetadatas || res.Secrets || res.SecretList || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "SSM_SECRETS", id: x.SecretId || x.SecretName }));
    },
    // Captcha apps
    async CAPTCHA() {
      const client = getClient("captcha", "v20190722", "captcha.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeCaptchaUserAllAppId?.({ Offset, Limit }) || {};
        const list = res.Data?.AllAppIdInfo || res.AppIds || [];
        return { items: list };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CAPTCHA", id: x.AppId || x.AppName }));
    },
    // TI-ONE notebook instances
    async TIONE() {
      const client = getClient("tione", "v20191022", "tione.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeNotebookInstances?.({ Offset, Limit }) || {};
        return { items: res.NotebookInstanceSet || res.NotebookInstances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TIONE", id: x.NotebookInstanceId || x.NotebookInstanceName }));
    },
    // SES identities
    async SES() {
      const client = getClient("ses", "v20201002", "ses.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.ListIdentities({ Offset, Limit });
        const arr = res.EmailIdentities || res.Identities || [];
        return { items: Array.isArray(arr) ? arr.map((v) => (typeof v === "string" ? { Identity: v } : v)) : [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "SES", id: x.Identity || x.IdentityName }));
    },
    // WeData projects
    async WEDATA() {
      const client = getClient("wedata", "v20210820", "wedata.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeProjects({ Offset, Limit });
        return { items: res.ProjectList || res.Projects || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "WEDATA", id: x.ProjectId || x.ProjectName }));
    },
    // EMR clusters
    async EMR() {
      try {
        const client = getClient("emr", "v20190103", "emr.tencentcloudapi.com");
        const items = await pagedFetch(async ({ Offset, Limit }) => {
          const res = await client.DescribeInstances({ Offset, Limit });
          return { items: res.ClusterList || res.Result || [] };
        });
        return items.filter(hasNoTags).map((x) => ({ service: "EMR", id: x.ClusterId || x.InstanceId }));
      } catch {
        return [];
      }
    },
    // Elasticsearch instances
    async ELASTICSEARCH() {
      const client = getClient("es", "v20180416", "es.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceList || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "ELASTICSEARCH", id: x.InstanceId }));
    },
  };
}

/* ====== CSV export to COS ======
   Writes the consolidated scan results as CSV to a COS bucket.
   Requires cos:PutObject on the target bucket/prefix when credentials are provided.
*/
async function exportCsvToCos(items, { bucket, region, prefix = "scan" }) {
  if (!ENV_SECRET_ID || !ENV_SECRET_KEY) {
    throw new Error("Missing COS credentials (ENV_SECRET_ID/ENV_SECRET_KEY)");
  }
  const cos = new COS({
    SecretId: ENV_SECRET_ID,
    SecretKey: ENV_SECRET_KEY,
    SecurityToken: ENV_TOKEN,
  });
  const header = "region,service,id\n";
  const lines = items.map((i) => `${i.region},${i.service},${i.id}`).join("\n");
  const csv = header + lines + (lines ? "\n" : "");
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const key = `${prefix}/scan-${ts}.csv`;
  await new Promise((resolve, reject) => {
    cos.putObject(
      { Bucket: bucket, Region: region, Key: key, Body: Buffer.from(csv, "utf8"), ContentType: "text/csv" },
      (err, data) => (err ? reject(err) : resolve(data))
    );
  });

}

/* ====== COS (global) scanner ======
   Lists COS buckets and checks for missing tags.
   COS SDK requires explicit credentials (env); SCF role is not auto-wired by this SDK.
*/
async function scanCOS() {
  // COS SDK requires explicit keys; role-based auth is not auto-wired here.
  if (!ENV_SECRET_ID || !ENV_SECRET_KEY) {
    return [];
  }

  const cos = new COS({
    SecretId: ENV_SECRET_ID,
    SecretKey: ENV_SECRET_KEY,
    SecurityToken: ENV_TOKEN,
  });

  // Some SDK responses return TagSet, others return { Tags: { Tag: [] } }.
  const normalizeCosTagSet = (data) => {
    const maybe =
      data?.TagSet ||
      data?.Tags?.Tag ||
      data?.Tags ||
      [];
    return Array.isArray(maybe) ? maybe : [];
  };

  // List all buckets visible to the provided credentials
  const buckets = await new Promise((resolve, reject) => {
    cos.getService({}, (err, data) => (err ? reject(err) : resolve(data)));
  }).then((d) => d.Buckets || []);

  const results = [];
  const concurrency = 5;
  let i = 0;

  // Parallel worker to fetch per-bucket tag info with limited concurrency
  async function worker() {
    while (i < buckets.length) {
      const idx = i++;
      const b = buckets[idx];
      const bucket = b.Name;
      const region = b.Location;

      try {
        const tagsRes = await new Promise((resolve, reject) => {
          cos.getBucketTagging({ Bucket: bucket, Region: region }, (err, data) => {
            if (err) {
              // If tagging not set, COS returns 404 or NoSuchTagSet
              if (
                err?.statusCode === 404 ||
                String(err?.errorCode || "").toLowerCase() === "nosuchtagset"
              ) {
                resolve({ TagSet: [] });
              } else {
                reject(err);
              }
            } else {
              resolve(data || { TagSet: [] });
            }
          });
        });

        const tagSet = normalizeCosTagSet(tagsRes);

        // Make it consistent with other scanners by using hasNoTags on an object
        if (hasNoTags({ TagSet: tagSet })) {
          results.push({ service: "COS", id: bucket, region: "global" });
        }
      } catch {
        // ignore errors and continue to next bucket
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, buckets.length) }, () => worker())
  );

  return results;
}

/* ====== Entry point ======
   Orchestrates the scan across regions and services, then optionally exports results to COS.
*/
exports.main_handler = async () => {
  const regions = await listAllRegions(); // resolved from env override or DescribeRegions
  
  const maxRegionConcurrency = parsedMaxRegionConc;

  // All available per-region scanners (use SCAN_SERVICES to subset)
  const allRegionServices = [
    "CVM",
    "CBS",
    "CLB",
    "SCF",
    "TKE",
    "TKE_SERVERLESS",
    "TCR",
    "BANDWIDTH_PACK",
    "VPN",
    "CCN",
    "NAT_GATEWAY",
    "EIP",
    "LIGHTHOUSE",
    "CLS",
    "ANTIDDOS",
    "TDMQ_CKAFKA",
    "TDMQ_ROCKETMQ",
    "TDMQ_RABBITMQ",
    "TDMQ_PULSAR",
    "CLOUD_FIREWALL",
    "MYSQL",
    "MSSQL",
    "POSTGRES",
    "TDSQL",
    "CYNOSDB",
    "REDIS",
    "MONGODB",
    "TEM",
    "PRIVATE_DNS",
    "ADP",
    "CSS_DOMAINS",
    "GAAP_GROUP",
    "CTSDB",
    "TENDIS",
    "VECTORDB",
    "DLC",
    "TCHOUSE_C",
    "TCHOUSE_P",
    "TCHOUSE_D",
    "KMS_KEYS",
    "SSM_SECRETS",
    "CAPTCHA",
    "TIONE",
    "SES",
    "WEDATA",
    "EMR",
    "ELASTICSEARCH",
  ];
  const regionServices = (SCAN_SERVICES && SCAN_SERVICES.trim())
    ? SCAN_SERVICES.split(",").map(s => s.trim()).filter(Boolean)
    : allRegionServices;
  let rIndex = 0;
  let untaggedCount = 0;
  const outputs = [];

  // Region worker runs selected service scanners concurrently per region
  async function regionWorker() {
    while (rIndex < regions.length) {
      const region = regions[rIndex++];
      const scan = scannersForRegion(region);
      const tasks = regionServices.map(async (svcName) => {
        const fn = scan[svcName];
        if (!fn) return;
        try {
          const items = await withTimeout(fn(), parsedServiceTimeout, `svc:${svcName}:${region}`);
          for (const it of items) {
            outputs.push({ service: it.service, id: it.id, region: it.region ?? region });
            untaggedCount++;
          }
        } catch {
          // timeout or error -> skip this service/region
        }
      });
      await Promise.allSettled(tasks);
    }
  }

  await Promise.all(Array.from({ length: Math.min(maxRegionConcurrency, regions.length) }, () => regionWorker()));

  // COS is global (scan once). Requires ENABLE_COS_SCAN="true" and valid COS read permissions.
  try {
    const cosItems = await scanCOS();
    for (const it of cosItems) {

      outputs.push({ service: it.service, id: it.id, region: it.region });
      untaggedCount++;
    }
  } catch {
    // ignore COS errors
  }

  // Sort results for stable CSV output
  const sortedOutputs = outputs.slice().sort((a, b) => {
    const r = String(a.region).localeCompare(String(b.region));
    if (r !== 0) return r;
    const s = String(a.service).localeCompare(String(b.service));
    if (s !== 0) return s;
    return String(a.id).localeCompare(String(b.id));
  });

  // Attempt CSV export (best-effort; failure does not crash the function)
  try {
    await exportCsvToCos(sortedOutputs, {
      bucket: "tommywork-1301327510",
      region: "eu-frankfurt",
      prefix: "scan",
    });
  } catch (e) {
    // ignore export errors
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ scannedRegions: regions.length, untaggedCount }),
  };
};