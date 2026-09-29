# data_helpers.py
# Plain Python utility module stored as a Workspace File

def format_number(value, prefix="", suffix=""):
    """Format a number with thousands separator and optional prefix/suffix."""
    return f"{prefix}{value:,.2f}{suffix}"

def log_pipeline_step(step_name, input_count, output_count):
    """Print a formatted pipeline step summary."""
    pct = (output_count / input_count * 100) if input_count > 0 else 0
    print(f"[PIPELINE] {step_name}")
    print(f"  Input rows : {input_count:>10,}")
    print(f"  Output rows: {output_count:>10,}  ({pct:.1f}% retained)")

def get_pipeline_config(env="development"):
    """Return environment-specific pipeline configuration."""
    configs = {
        "development": {
            "raw_path"   : "/FileStore/dbx-de-course/raw/",
            "silver_path": "/FileStore/dbx-de-course/silver/",
            "gold_path"  : "/FileStore/dbx-de-course/gold/",
            "log_level"  : "DEBUG"
        },
        "production": {
            "raw_path"   : "s3://my-bucket/raw/",
            "silver_path": "s3://my-bucket/silver/",
            "gold_path"  : "s3://my-bucket/gold/",
            "log_level"  : "INFO"
        }
    }
    return configs.get(env, configs["development"])