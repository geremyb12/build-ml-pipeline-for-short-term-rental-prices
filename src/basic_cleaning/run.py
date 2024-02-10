#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Load the data
    df = pd.read_csv(artifact_local_path)

    # Apply basic data cleaning
    logger.info("Applying basic data cleaning...")
    # Drop outliers based on min_price and max_price
    df = df[(df["price"] >= args.min_price) & (df["price"] <= args.max_price)]

    # Save the cleaned data to CSV
    logger.info("Saving cleaned data to CSV...")
    df.to_csv("clean_sample.csv", index=False)

    # Upload cleaned data as a new artifact
    logger.info("Uploading cleaned data to W&B...")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    logger.info("Basic cleaning step completed successfully.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Name of the input artifact containing the raw dataset",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the output artifact containing the cleaned dataset",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description for the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum price to consider for outliers removal",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum price to consider for outliers removal",
        required=True
    )

    args = parser.parse_args()

    go(args)