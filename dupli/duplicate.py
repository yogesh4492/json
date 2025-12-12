import boto3
import imagehash
from PIL import Image
from io import BytesIO
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from collections import defaultdict


# ======================================================
# CONFIGURATION
# ======================================================
S3_BUCKET = "prod-shaip-bucket"
S3_BASE_PATH = "projects/row530/data-delivery/"

OUTPUT_CSV = "s3_duplicate_images.csv"
FAILED_CSV = "s3_failed_images.csv"

MAX_WORKERS = 5           # Stable parallelism
HASH_SIZE = 8             # Phash resolution
SLEEP_BETWEEN_READS = 0.1 # Reduce S3 throttling

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp,".heic',".HEIC"}

s3_client = boto3.client(
    's3',
    config=boto3.session.Config(
        read_timeout=60,
        connect_timeout=60,
        retries={'max_attempts': 3}
    )
)


# ======================================================
# HELPERS
# ======================================================
def extract_batch_name(key):
    """Return folder containing 'Batch' text."""
    for part in key.split('/'):
        if "batch" in part.lower():
            return part
    return "Unknown"


def is_image_file(key):
    return any(key.lower().endswith(ext) for ext in IMAGE_EXTENSIONS)


# ======================================================
# S3 LISTING
# ======================================================
def list_all_s3_files(bucket, prefix):
    print(f"📂 Scanning S3 path: s3://{bucket}/{prefix}")
    all_files = []

    paginator = s3_client.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            for item in contents:
                key = item["Key"]
                if is_image_file(key):
                    all_files.append({
                        "key": key,
                        "size": item["Size"],
                        "batch": extract_batch_name(key)
                    })
    except Exception as e:
        print(f"❌ Error listing files: {e}")
        return []

    print(f"✅ Found {len(all_files)} images\n")
    return all_files


# ======================================================
# HASHING
# ======================================================
def compute_image_hash(file_info, retry=0, max_retry=2):
    key = file_info["key"]
    filename = key.split("/")[-1]
    batch = file_info["batch"]

    try:
        time.sleep(SLEEP_BETWEEN_READS)

        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        data = response["Body"].read()

        if not data:
            raise ValueError("Empty S3 file")

        img = Image.open(BytesIO(data))

        # Ensure correct mode
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        hash_val = str(imagehash.phash(img, hash_size=HASH_SIZE))

        print(f"✅ [{batch}] {filename}")

        return {
            "success": True,
            "key": key,
            "filename": filename,
            "hash": hash_val,
            "batch": batch,
            "file_size": file_info["size"],
            "s3_path": f"s3://{S3_BUCKET}/{key}"
        }

    except Exception as e:
        if retry < max_retry:
            print(f"⚠️  [{batch}] {filename} retry {retry + 1}/{max_retry}")
            time.sleep(1)
            return compute_image_hash(file_info, retry + 1, max_retry)

        print(f"❌ [{batch}] {filename} - {e}")
        return {
            "success": False,
            "key": key,
            "filename": filename,
            "hash": None,
            "batch": batch,
            "file_size": file_info["size"],
            "s3_path": f"s3://{S3_BUCKET}/{key}",
            "error": str(e)
        }


# ======================================================
# DUPLICATE FINDER
# ======================================================
def find_duplicates(results):
    groups = defaultdict(list)

    for r in results:
        if r["success"] and r["hash"]:
            groups[r["hash"]].append(r)

    duplicates = []

    for h, files in groups.items():
        if len(files) > 1:
            files = sorted(files, key=lambda x: x["batch"])
            original = files[0]

            for d in files[1:]:
                duplicates.append({
                    "Duplicate_File": d["filename"],
                    "Duplicate_S3_Path": d["s3_path"],
                    "Duplicate_Batch": d["batch"],
                    "Duplicate_Size": d["file_size"],
                    "Original_File": original["filename"],
                    "Original_S3_Path": original["s3_path"],
                    "Original_Batch": original["batch"],
                    "Original_Size": original["file_size"],
                    "Hash": h
                })

    return duplicates


# ======================================================
# MAIN WORKFLOW
# ======================================================
def find_s3_duplicates():
    print("=" * 70)
    print("🚀 S3 IMAGE DUPLICATE DETECTION (Optimized & Stable)")
    print("=" * 70)

    start = time.time()

    files = list_all_s3_files(S3_BUCKET, S3_BASE_PATH)
    if not files:
        print("❌ No images found.")
        return

    print(f"⚙️ Hashing {len(files)} images using {MAX_WORKERS} threads...\n")

    results = []
    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(compute_image_hash, f): f for f in files}

        for idx, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            results.append(result)

            if result["success"]:
                success += 1
            else:
                failed += 1

            if idx % 200 == 0:
                print(f"📊 Progress {idx}/{len(files)} | "
                      f"Success: {success} | Fail: {failed}")

    # ----------------------------
    # DUPLICATE CALCULATION
    # ----------------------------
    dupes = find_duplicates(results)

    if dupes:
        df = pd.DataFrame(dupes)
        df.to_csv(OUTPUT_CSV, index=False)

        print("\n🎯 Duplicate Detection Complete")
        print(f"🔴 Found {len(dupes)} duplicates")
        print(f"📁 Saved to {OUTPUT_CSV}")

        print("\n📊 Duplicate Count Per Batch:")
        print(df.groupby("Duplicate_Batch").size())

    else:
        print("\n🎉 No duplicates found!")

    # ----------------------------
    # SAVE FAILED
    # ----------------------------
    if failed:
        fail_df = pd.DataFrame([x for x in results if not x["success"]])
        fail_df.to_csv(FAILED_CSV, index=False)
        print(f"\n⚠️ Failed images saved to {FAILED_CSV}")

    print(f"\n⏱ Total Time: {(time.time() - start)/60:.2f} min")


# ======================================================
# EXECUTION
# ======================================================
if __name__ == "__main__":
    try:
        find_s3_duplicates()
    except KeyboardInterrupt:
        print("⛔ Interrupted by user.")
