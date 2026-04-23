#!/usr/bin/env python
"""
Performs basic cleaning on the data and saves the results in W&B
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
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    
    
    # Download input artifact
    artifact = run.use_artifact(args.input_artifact)
    df = pd.read_csv(artifact.file())

    # Basic cleaning
    df = df.drop_duplicates()
    df = df.dropna(subset=["price"])
    df = df[df["price"].between(args.min_price, args.max_price)]
    df["last_review"] = pd.to_datetime(df["last_review"])
    
    # Add this boundary filter
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    logger.info("Cleaned data has %s rows and %s columns", *df.shape)

    # Save cleaned data
    df.to_csv("clean_sample.csv", index=False)

    # Upload artifact
    cleaned_artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    cleaned_artifact.add_file("clean_sample.csv")
    run.log_artifact(cleaned_artifact)

    run.finish()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Input raw data artifact (e.g. sample.csv:latest)",
        required=True,
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name of the output cleaned dataset artifact",
        required=True,
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type of the output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description of the cleaned dataset",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum allowed price for listings",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum allowed price for listings",
        required=True,
    )

    args = parser.parse_args()
    
    go(args)
