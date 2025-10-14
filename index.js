/* Copied from scf-untagged-scan.js as SCF entrypoint */
const tencentcloud = require("tencentcloud-sdk-nodejs");
const COS = require("cos-nodejs-sdk-v5");

/* ====== Config ====== */
const { TENCENTCLOUD_REGION = "eu-frankfurt" } = process.env;
// Support SCF-provided env names and user-provided variants
const ENV_SECRET_ID =
  process.env.TENCENTCLOUD_SECRETID || process.env.TENCENTCLOUD_SECRET_ID;
const ENV_SECRET_KEY =
  process.env.TENCENTCLOUD_SECRETKEY || process.env.TENCENTCLOUD_SECRET_KEY;
const ENV_TOKEN =
  process.env.TENCENTCLOUD_SESSIONTOKEN || process.env.TENCENTCLOUD_SESSION_TOKEN;

if (!ENV_SECRET_ID || !ENV_SECRET_KEY) {
  
}

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

/* ====== Helpers ====== */
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
const hasNoTags = (obj) => {
  const tags = extractTags(obj);
  return !tags || tags.length === 0;
};

async function listAllRegions() {
  const CvmClient = tencentcloud.cvm.v20170312.Client;
  const client = new CvmClient({
    ...baseConfig(TENCENTCLOUD_REGION),
    profile: { httpProfile: { endpoint: "cvm.tencentcloudapi.com" } },
  });
  const res = await client.DescribeRegions({});
  const regs = (res.RegionSet || []).map((r) => r.Region).filter(Boolean);
  return Array.from(new Set(regs));
}

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

/* ====== Region-scoped scanners ====== */
function scannersForRegion(region) {
  const clients = {};
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
    async CVM() {
      const client = getClient("cvm", "v20170312", "cvm.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CVM", id: x.InstanceId }));
    },
    async CBS() {
      const client = getClient("cbs", "v20170312", "cbs.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDisks({ Offset, Limit });
        return { items: res.DiskSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CBS", id: x.DiskId }));
    },
    async CLB() {
      const client = getClient("clb", "v20180317", "clb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeLoadBalancers({ Offset, Limit });
        return { items: res.LoadBalancerSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CLB", id: x.LoadBalancerId }));
    },
    async SCF() {
      const client = getClient("scf", "v20180416", "scf.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.ListFunctions({ Offset, Limit });
        return { items: res.Functions || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "SCF", id: x.FunctionName }));
    },
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
    async TCR() {
      const client = getClient("tcr", "v20190924", "tcr.tencentcloudapi.com");
      const res = await client.DescribeInstances({});
      const items = res.Registries || res.Instances || [];
      return items.filter(hasNoTags).map((x) => ({ service: "TCR", id: x.RegistryId || x.InstanceId }));
    },
    async BANDWIDTH_PACK() {
      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeBandwidthPackages({ Offset, Limit });
        return { items: res.BandwidthPackageSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "BANDWIDTH_PACK", id: x.BandwidthPackageId }));
    },
    async VPN() {
      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeVpnGateways({ Offset, Limit });
        return { items: res.VpnGatewaySet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "VPN", id: x.VpnGatewayId }));
    },
    async CCN() {
      // CCN is global; report only once from the Frankfurt region to avoid duplicates
      if (region !== "eu-frankfurt") return [];

      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeCcns({ Offset, Limit });
        return { items: res.CcnSet || res.Ccns || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CCN", id: x.CcnId }));
    },
    async NAT_GATEWAY() {
      const client = getClient("vpc", "v20170312", "vpc.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeNatGateways({ Offset, Limit });
        return { items: res.NatGatewaySet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "NAT_GATEWAY", id: x.NatGatewayId }));
    },
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
    async LIGHTHOUSE() {
      const client = getClient("lighthouse", "v20200324", "lighthouse.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "LIGHTHOUSE", id: x.InstanceId }));
    },
    async CLS() {
      const client = getClient("cls", "v20201016", "cls.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeLogsets({ Offset, Limit });
        return { items: res.Logsets || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "CLS", id: x.LogsetId }));
    },
    async ANTIDDOS() {
      const client = getClient("antiddos", "v20200309", "antiddos.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeListBGPInstances({ Offset, Limit });
        return { items: res.Data?.List || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "ANTIDDOS", id: x.InstanceId }));
    },
    async TDMQ_CKAFKA() {
      const client = getClient("ckafka", "v20190819", "ckafka.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceList || res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_CKAFKA", id: x.InstanceId }));
    },
    async TDMQ_ROCKETMQ() {
      const client = getClient("tdmq", "v20200217", "tdmq.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeRocketMQClusters({ Offset, Limit });
        return { items: res.ClusterInfoList || res.ClusterList || res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_ROCKETMQ", id: x.ClusterId || x.InstanceId }));
    },
    async TDMQ_RABBITMQ() {
      const client = getClient("tdmq", "v20200217", "tdmq.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeRabbitMQServerlessInstances({ Offset, Limit });
        return { items: res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_RABBITMQ", id: x.InstanceId || x.ClusterId }));
    },
    async TDMQ_PULSAR() {
      const client = getClient("tdmq", "v20200217", "tdmq.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribePulsarProInstances({ Offset, Limit });
        return { items: res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDMQ_PULSAR", id: x.InstanceId }));
    },
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
    async MYSQL() {
      const client = getClient("cdb", "v20170320", "cdb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.Items || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "MYSQL", id: x.InstanceId }));
    },
    async MSSQL() {
      const client = getClient("sqlserver", "v20180328", "sqlserver.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.DBInstances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "MSSQL", id: x.InstanceId }));
    },
    async POSTGRES() {
      const client = getClient("postgres", "v20170312", "postgres.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.DBInstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "POSTGRES", id: x.DBInstanceId }));
    },
    async TDSQL() {
      const client = getClient("dcdb", "v20180411", "dcdb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDCDBInstances({ Offset, Limit });
        return { items: res.Instances || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDSQL", id: x.InstanceId }));
    },
    async CYNOSDB() {
      const client = getClient("cynosdb", "v20190107", "cynosdb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeClusters({ Offset, Limit });
        return { items: res.ClusterSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "TDSQL_C", id: x.ClusterId }));
    },
    async REDIS() {
      const client = getClient("redis", "v20180412", "redis.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeInstances({ Offset, Limit });
        return { items: res.InstanceSet || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "REDIS", id: x.InstanceId }));
    },
    async MONGODB() {
      const client = getClient("mongodb", "v20190725", "mongodb.tencentcloudapi.com");
      const items = await pagedFetch(async ({ Offset, Limit }) => {
        const res = await client.DescribeDBInstances({ Offset, Limit });
        return { items: res.InstanceDetails || res.InstanceList || [] };
      });
      return items.filter(hasNoTags).map((x) => ({ service: "MONGODB", id: x.InstanceId || x.InstanceName }));
    },
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

/* ====== CSV export to COS ====== */
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

/* ====== COS (global) scanner ====== */
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

  const buckets = await new Promise((resolve, reject) => {
    cos.getService({}, (err, data) => (err ? reject(err) : resolve(data)));
  }).then((d) => d.Buckets || []);

  const results = [];
  const concurrency = 5;
  let i = 0;

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
          results.push({ service: "COS", id: bucket, region });
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

/* ====== Entry point ====== */
exports.main_handler = async () => {
  const regions = await listAllRegions();

  const regionServices = [
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
    "EMR",
    "ELASTICSEARCH",
  ];

  const maxRegionConcurrency = 3;
  let rIndex = 0;
  let untaggedCount = 0;
  const outputs = [];

  async function regionWorker() {
    while (rIndex < regions.length) {
      const region = regions[rIndex++];
      const scan = scannersForRegion(region);
      const tasks = regionServices.map(async (svcName) => {
        const fn = scan[svcName];
        if (!fn) return;
        try {
          const items = await fn();
          for (const it of items) {

            outputs.push({ service: it.service, id: it.id, region });
            untaggedCount++;
          }
        } catch {
          // ignore service/region errors to keep scanning
        }
      });
      await Promise.all(tasks);
    }
  }

  await Promise.all(Array.from({ length: Math.min(maxRegionConcurrency, regions.length) }, () => regionWorker()));

  // COS is global (scan once)
  try {
    const cosItems = await scanCOS();
    for (const it of cosItems) {

      outputs.push({ service: it.service, id: it.id, region: it.region });
      untaggedCount++;
    }
  } catch {
    // ignore COS errors
  }

  const sortedOutputs = outputs.slice().sort((a, b) => {
    const r = String(a.region).localeCompare(String(b.region));
    if (r !== 0) return r;
    const s = String(a.service).localeCompare(String(b.service));
    if (s !== 0) return s;
    return String(a.id).localeCompare(String(b.id));
  });

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