import boto3
import hashlib
import pandas as pd
from urllib.parse import urlparse
import tempfile
from tqdm import tqdm
import typer
from concurrent.futures import ThreadPoolExecutor, as_completed

app = typer.Typer(help="Check duplicate files (by name and content) in an S3 path using parallel hashing.")


def get_s3_files(bucket_name: str, prefix: str):
    """List all files from the given S3 path."""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    file_list = []

    typer.echo("📋 Listing files from S3...")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            if not obj['Key'].endswith('/'):  # skip folder entries
                file_list.append({
                    'Key': obj['Key'],
                    'Size': obj['Size'],
                    'ETag': obj.get('ETag', '').strip('"')
                })
    return file_list


def compute_partial_md5(bucket: str, key: str, s3_client, size: int, sample_size: int = 1024 * 1024):
    """Compute MD5 of first and last chunks for quick comparison."""
    md5 = hashlib.md5()
    
    try:
        # Read first chunk
        response = s3_client.get_object(Bucket=bucket, Key=key, Range=f'bytes=0-{sample_size-1}')
        first_chunk = response['Body'].read()
        md5.update(first_chunk)
        
        # Read last chunk if file is large enough
        if size > sample_size:
            last_start = max(0, size - sample_size)
            response = s3_client.get_object(Bucket=bucket, Key=key, Range=f'bytes={last_start}-{size-1}')
            last_chunk = response['Body'].read()
            md5.update(last_chunk)
        
        return md5.hexdigest()
    except Exception as e:
        return None


def compute_full_md5(bucket: str, key: str, s3_client, chunk_size: int = 8 * 1024 * 1024):
    """Download S3 file temporarily and compute full MD5 hash."""
    md5 = hashlib.md5()
    try:
        with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
            s3_client.download_file(bucket, key, tmp_file.name)
            with open(tmp_file.name, 'rb') as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    md5.update(chunk)
        return md5.hexdigest()
    except Exception as e:
        return None


def parallel_hash(bucket: str, file_list, max_workers: int = 16, use_partial: bool = True):
    """Compute hashes in parallel using ThreadPoolExecutor."""
    s3 = boto3.client('s3')
    results = [None] * len(file_list)

    hash_type = "partial (fast)" if use_partial else "full"
    typer.echo(f"\n🚀 Starting parallel {hash_type} hashing with {max_workers} threads for {len(file_list)} files...\n")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {}
        for i in range(len(file_list)):
            if use_partial:
                future = executor.submit(compute_partial_md5, bucket, file_list[i]['Key'], s3, file_list[i]['Size'])
            else:
                future = executor.submit(compute_full_md5, bucket, file_list[i]['Key'], s3)
            future_to_index[future] = i

        for future in tqdm(as_completed(future_to_index), total=len(file_list), desc="Hashing files", unit="file"):
            i = future_to_index[future]
            try:
                results[i] = future.result()
            except Exception as e:
                typer.echo(f"⚠️ Error hashing {file_list[i]['Key']}: {e}")
                results[i] = None

    return results


def find_potential_duplicates_by_size_etag(df: pd.DataFrame):
    """Quick pre-filter using size and ETag to find potential duplicates."""
    # Group by size first (very fast)
    size_groups = df.groupby('Size').filter(lambda x: len(x) > 1)
    
    if size_groups.empty:
        return pd.DataFrame(), df
    
    # Return both potential duplicates and unique-sized files
    unique_sizes = df[~df.index.isin(size_groups.index)]
    
    typer.echo(f"📊 Found {len(size_groups)} files with matching sizes (potential duplicates)")
    typer.echo(f"📊 Found {len(unique_sizes)} files with unique sizes (definitely not duplicates)")
    return size_groups, unique_sizes


def create_duplicate_rows(df: pd.DataFrame, group_by_col: str):
    """Create rows showing original file and all its duplicates in same row."""
    duplicate_mask = df.duplicated(group_by_col, keep=False) & df[group_by_col].notna()
    dupes = df[duplicate_mask].copy()
    
    if dupes.empty:
        return pd.DataFrame()
    
    # Group by the duplicate key
    grouped = dupes.groupby(group_by_col)
    
    result_rows = []
    for name, group in grouped:
        # Sort by Key to have consistent ordering
        group = group.sort_values('Key').reset_index(drop=True)
        
        # First file is the "original"
        row = {
            'Original_File': group.loc[0, 'Key'],
            'Original_Size': group.loc[0, 'Size'],
            'Hash': group.loc[0, group_by_col],
            'Total_Duplicates': len(group) - 1
        }
        
        # Add all duplicate files
        for i in range(1, len(group)):
            row[f'Duplicate_{i}_File'] = group.loc[i, 'Key']
            row[f'Duplicate_{i}_Size'] = group.loc[i, 'Size']
        
        result_rows.append(row)
    
    return pd.DataFrame(result_rows)


def save_to_csv(name_dupes, content_dupes, output_file: str):
    """Save duplicate results to separate CSV files."""
    base_name = output_file.replace('.xlsx', '').replace('.csv', '')
    name_file = f"{base_name}_name_duplicates.csv"
    content_file = f"{base_name}_content_duplicates.csv"

    if not name_dupes.empty:
        name_dupes.to_csv(name_file, index=False)
        typer.echo(f"   📄 Name duplicates: {name_file} ({len(name_dupes)} groups)")
    
    if not content_dupes.empty:
        content_dupes.to_csv(content_file, index=False)
        typer.echo(f"   📄 Content duplicates: {content_file} ({len(content_dupes)} groups)")

    typer.echo(f"\n✅ Duplicate reports saved!")


def parse_s3_path(s3_path: str):
    """Split s3://bucket/prefix into bucket and prefix."""
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip('/')


@app.command()
def check(
    s3path: str = typer.Argument(..., help="S3 path, e.g., s3://my-bucket/folder/"),
    output: str = typer.Option("s3_duplicates.csv", "--output", "-o", help="Base name for output CSV files"),
    threads: int = typer.Option(16, "--threads", "-t", help="Number of threads for parallel hashing"),
    full_hash: bool = typer.Option(False, "--full-hash", help="Use full file hash (slower but more accurate)"),
    skip_content: bool = typer.Option(False, "--skip-content", help="Skip content duplicate check (only check names)"),
):
    """Check for duplicate file names and content in an S3 path (optimized for large datasets)."""
    bucket, prefix = parse_s3_path(s3path)
    typer.echo(f"\n📂 Scanning bucket: {bucket}, prefix: {prefix}\n")

    files = get_s3_files(bucket, prefix)
    if not files:
        typer.echo("❌ No files found in given S3 path.")
        raise typer.Exit()

    typer.echo(f"✅ Found {len(files)} files\n")
    
    # Create DataFrame
    df = pd.DataFrame(files)
    
    # Check name duplicates (very fast)
    typer.echo("🔍 Checking for name duplicates...")
    name_dupes = create_duplicate_rows(df, 'Key')
    
    content_dupes = pd.DataFrame()
    if not skip_content:
        # Pre-filter by size to reduce hashing workload
        typer.echo("\n🔍 Pre-filtering by file size...")
        potential_dupes, unique_sizes = find_potential_duplicates_by_size_etag(df)
        
        if not potential_dupes.empty:
            # Only hash files that have matching sizes
            typer.echo(f"\n⚡ Hashing {len(potential_dupes)} potential duplicate files (skipping {len(unique_sizes)} unique sizes)")
            
            # Convert to list of dicts for hashing
            files_to_hash = potential_dupes.to_dict('records')
            
            # Use partial hash by default for speed
            md5_hashes = parallel_hash(bucket, files_to_hash, threads, use_partial=not full_hash)
            potential_dupes = potential_dupes.copy()
            potential_dupes['ContentHash'] = md5_hashes
            
            # Find content duplicates
            content_dupes = create_duplicate_rows(potential_dupes, 'ContentHash')
        else:
            typer.echo("\n✅ No files with matching sizes found - no content duplicates possible")

    # Summary
    typer.echo("\n" + "="*60)
    if name_dupes.empty and content_dupes.empty:
        typer.echo("✅ No duplicates found!")
    else:
        typer.echo(f"📊 Duplicate Summary:")
        if not name_dupes.empty:
            total_name_dupes = name_dupes['Total_Duplicates'].sum()
            typer.echo(f"   🔤 Name duplicates: {len(name_dupes)} original files with {int(total_name_dupes)} duplicates")
        if not content_dupes.empty:
            total_content_dupes = content_dupes['Total_Duplicates'].sum()
            typer.echo(f"   🔐 Content duplicates: {len(content_dupes)} original files with {int(total_content_dupes)} duplicates")
        typer.echo("")
        save_to_csv(name_dupes, content_dupes, output)


if __name__ == "__main__":
    app()