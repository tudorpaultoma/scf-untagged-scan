# SCF Untagged Resource Scanner

This Node.js script scans Tencent Cloud resources across all regions and identifies items without tags. It then exports a CSV report to a COS bucket.

## What it scans
Across each region:
- CVM (instances)
- CBS (disks)
- CLB (load balancers)
- SCF (functions)
- TKE (standard + serverless)
- TCR (registries)
- VPC Bandwidth Packages
- VPN Gateways
- NAT Gateway
- EIP (unbounded public IPs)
- CCN
- Lighthouse (instances)
- CLS (logsets)
- AntiDDoS (BGP instances)
- TDMQ (CKafka, RocketMQ, RabbitMQ, Pulsar)
- Cloud Firewall (NAT firewall instances)
- Databases: MySQL (CDB), SQL Server, Postgres, TDSQL, CynosDB
- Redis
- MongoDB
- TEM
- PRIVATE_DNS
- ADP
- CSS_DOMAINS
- GAAP_GROUP
- CTSDB
- TENDIS
- VECTORDB
- DLC
- TCHOUSE_C
- TCHOUSE_P
- TCHOUSE_D
- KMS_KEYS
- SSM_SECRETS
- CAPTCHA
- TIONE
- SES
- WEDATA
- EMR
- Elasticsearch

Global (once):
- COS buckets

## Output
- COS: A CSV stored at `scan/scan-<timestamp>.csv` with header:
  ```
  region,service,id
  ```
- Function return: `{ scannedRegions: number, untaggedCount: number }`.

## Requirements
- Node.js 16+ (if running locally)
- Packages:
  - `tencentcloud-sdk-nodejs`
  - `cos-nodejs-sdk-v5`
- Tencent Cloud account with permissions to read the listed services and write to the target COS bucket.

## Configuration (Environment Variables)
Supports SCF defaults and user variants:
- `TENCENTCLOUD_REGION` (default: `eu-frankfurt`)
- `TENCENTCLOUD_SECRETID` or `TENCENTCLOUD_SECRET_ID`
- `TENCENTCLOUD_SECRETKEY` or `TENCENTCLOUD_SECRET_KEY`
- `TENCENTCLOUD_SESSIONTOKEN` or `TENCENTCLOUD_SESSION_TOKEN` (optional)

Notes:
- For service scans in SCF, the SDK can use the function's role credentials if env secrets are not set.
- For COS operations, explicit `SecretId` and `SecretKey` are required (role-based auth is not auto-wired in the COS SDK). If missing, COS scanning/export is skipped or will error.

## COS export destination
Edit the parameters passed to `exportCsvToCos` in `index.js` inside `main_handler`:
```js
await exportCsvToCos(outputs, {
  bucket: "YOUR_BUCKET_NAME",
  region: "YOUR_BUCKET_REGION",
  prefix: "scan",
});
```

## Usage

### 1) Deploy on Tencent Cloud SCF (recommended)
- Runtime: Node.js
- Handler: `index.main_handler`
- Set environment variables as needed.
- IAM role permissions:
  - Read-only for the listed services (Describe* APIs).
  - COS: `GetService`, `GetBucketTagging`, `PutObject` on your target bucket.
- Invoke the function; CSV will be uploaded to COS, and the function returns summary JSON.

### 2) Run locally
1. Install deps:
   ```bash
   npm install tencentcloud-sdk-nodejs cos-nodejs-sdk-v5
   ```
2. Set environment variables (include COS keys for CSV export):
   ```bash
   export TENCENTCLOUD_SECRET_ID=xxx
   export TENCENTCLOUD_SECRET_KEY=yyy
   export TENCENTCLOUD_SESSION_TOKEN=zzz # if using temporary creds
   export TENCENTCLOUD_REGION=eu-frankfurt
   ```
3. Run the handler:
   ```bash
   node -e 'require("./index").main_handler().then(console.log).catch(console.error)'
   ```

## Behavior & Limitations
- Pagination handled uniformly (page size 100).
- Best-effort scanning: service/region errors are caught and ignored to continue scanning.
- COS scanning requires explicit keys; otherwise it returns an empty list.
- Tag extraction is heuristic across different API shapes; resources with any tag are considered tagged.

## Technical debt
 - don't forget to remove any hard references to bucket name in the code or region
 - add more tests
 - add more logging
 - add more services

## License
Proprietary or MIT — choose and update as needed.