"""Generate a named regression fixture: python -m datagen SCENARIO --out DIR."""
import argparse
from pathlib import Path

from . import parquet, scenarios, sparse

SCENARIOS = {
    name: getattr(scenarios, name) for name in (
        'bloom', 'census', 'count_certificate', 'decimal_exact', 'dotted_twin', 'full_match',
        'json_escape', 'list_int64', 'many_tiny', 'min_fill', 'mixed_codec', 'nested', 'nested_key',
        'null_pages', 'overflow_rows', 'packed_count', 'partagg_budget', 'repeat_keys', 'scalar_strings', 'lambda_int8',
        'skew_filter', 'spawn_budget', 'topk', 'topn_budget', 'topn_descending', 'topn_gather', 'typed_root',
    )
}
SCENARIOS['stats_order'] = parquet.stats_order


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario', metavar='SCENARIO', choices=[*sorted(SCENARIOS), 'sparse_parquet'])
    parser.epilog = 'Scenarios: ' + ', '.join([*sorted(SCENARIOS), 'sparse_parquet'])
    parser.add_argument('--out', type=Path, required=True, help='destination directory')
    parser.add_argument('--rows', type=int, help='row count for topn_descending or sparse_parquet')
    parser.add_argument('--target-size', type=sparse.parse_size, help='logical size for sparse_parquet, e.g. 4g')
    parser.add_argument('--duckdb', help='DuckDB CLI for sparse_parquet (default: duckdb)')
    parser.add_argument('--upload-bucket', help='explicit upload of nested_edges.parquet after generating nested')
    args = parser.parse_args()
    if args.rows is not None and (args.scenario not in ('topn_descending', 'sparse_parquet') or args.rows <= 0):
        parser.error('--rows requires topn_descending or sparse_parquet and a positive count')
    if args.target_size is not None and args.scenario != 'sparse_parquet':
        parser.error('--target-size requires sparse_parquet')
    if args.duckdb and args.scenario != 'sparse_parquet':
        parser.error('--duckdb requires sparse_parquet')
    if args.upload_bucket and args.scenario != 'nested':
        parser.error('--upload-bucket requires nested')
    args.out.mkdir(parents=True, exist_ok=True)
    if args.scenario == 'sparse_parquet':
        base = args.out / 'base.parquet'
        target = args.target_size if args.target_size is not None else 4 * 1024**3
        result = args.out / f'sparse_{target // (1024 * 1024)}m.parquet'
        sparse.make_base(base, args.rows or 1_000_000, args.duckdb or 'duckdb')
        gap = sparse.inflate_with_gap(base, result, target)
        print(f'{result}: logical={result.stat().st_size} allocated={sparse.allocated_bytes(result)} gap={gap}')
    else:
        if args.rows is None:
            SCENARIOS[args.scenario](args.out)
        else:
            SCENARIOS[args.scenario](args.out, args.rows)
    if args.upload_bucket:
        import boto3
        boto3.client('s3').put_object(Bucket=args.upload_bucket, Key='zpq_test_data/nested_edges.parquet',
                                    Body=(args.out / 'nested_edges.parquet').read_bytes())


if __name__ == '__main__':
    main()
