#!/usr/bin/env python

import RNA
from Bio import SeqIO
import polars as pl
import click


def select_chunk(chunk_df, structures):
    urs_selection = list(chunk_df.select(pl.col("urs")).to_numpy().flatten())
    selected_structures = (
        pl.scan_csv(structures, has_header=False)
        .rename(
            {
                "column_1": "urs",
                "column_2": "structure",
                "column_3": "model_id",
                "column_4": "type",
            }
        )
        .filter(pl.col("urs").is_in(urs_selection))
        .collect()
    )
    return selected_structures.join(chunk_df, on="urs")


def calculate_energy(arg):
    seq, struc = arg
    try:
        E = RNA.eval_structure_simple(seq, struc)
    except:
        return 100000.0
    if E == 100000.0:
        E = 100000.0

    return E


@click.command()
@click.argument("fasta")
@click.argument("structure")
@click.argument("output")
@click.option("--chunksize", default=1000)
def cli(fasta, structure, output, chunksize):
    sequences = SeqIO.index(fasta, "fasta")
    chunk = []
    feature_df = pl.DataFrame()
    for n, (urs, record) in enumerate(sequences.items()):
        if n > 0 and n % chunksize == 0:
            chunk_df = pl.DataFrame(chunk)
            full_df = select_chunk(chunk_df, structure)
            full_df = full_df.with_columns(
                full_df.select(["seq", "structure"])
                .apply(calculate_energy)["apply"]
                .alias("energy")
            )
            feature_df = feature_df.vstack(full_df)
            chunk = []
        seq = str(record.seq)
        chunk.append({"urs": urs, "seq": seq})

    feature_df.write_csv(output, has_header=False)


if __name__ == "__main__":
    cli()