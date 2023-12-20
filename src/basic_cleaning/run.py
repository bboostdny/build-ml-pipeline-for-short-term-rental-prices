#!/usr/bin/env python
"""
Download from W&B raw dataset and apply basic data cleaning, export result to a new artifact
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
    logger.info(f"Downloading artifact from W&B: {args.input_artifact} and reading data to a dataframe")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)
    logger.info("Cleaning input file...")
    logger.debug(f"Dropping outliers using min price {args.min_price} and max price {args.max_price}")
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    logger.debug("Converting last_review column to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])
    # logger.debug("Drop rows outside of defined geolocation")
    # idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    # df = df[idx].copy()
    logger.info("Input file cleaned, saving cleaned data")
    df.to_csv("clean_sample.csv", index=False)
    logger.info("Uploading cleaned data to W&B")
    artifact = wandb.Artifact(
        "clean_sample.csv",
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Full name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price per night - offers with prices lower than this value will be removed",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price per night - offers with prices higher than this value will be removed",
        required=True
    )


    args = parser.parse_args()

    go(args)
