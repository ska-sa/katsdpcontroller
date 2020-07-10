#!/usr/bin/env python3

from datetime import datetime, timezone
import hashlib
import io
import json

import numpy as np
import astropy.units as u
import astropy.table
import botocore.session

import katsdpmodels.band_mask
import katsdpmodels.rfi_mask


BUCKET_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": ["arn:aws:s3:::models", "arn:aws:s3:::models/*"],
        }
    ]
}


RFI_MASK = np.array(
    [(935.40e6, 960.05e6, 1000.0),
     (1145.85e6, 1300.90e6, 1000.0),
     (1519.35e6, 1608.30e6, 1000.0)],
    dtype=[('min_frequency', 'f8'), ('max_frequency', 'f8'), ('max_baseline', 'f8')]
)


BAND_MASK = np.array(
    [(0.0, 0.05), (0.95, 1.0)],
    dtype=[('min_fraction', 'f8'), ('max_fraction', 'f8')]
)


def serialize_model(model):
    fh = io.BytesIO()
    model.to_file(fh, content_type='application/x-hdf5')
    checksum = hashlib.sha256(fh.getvalue()).hexdigest()
    fh.seek(0)
    return fh, f'sha256_{checksum}.h5'


def put_rfi_mask(client):
    rfi_mask_table = astropy.table.Table(RFI_MASK)
    rfi_mask_table['min_frequency'].unit = u.Hz
    rfi_mask_table['max_frequency'].unit = u.Hz
    rfi_mask_table['max_baseline'].unit = u.m
    model = katsdpmodels.rfi_mask.RFIMaskRanges(rfi_mask_table, False)
    model.author = 'Sandbox setup script'
    model.comment = 'RFI mask model for use in sandbox testing'
    model.target = 'MeerKAT'
    model.created = datetime(2020, 6, 15, 14, 11, tzinfo=timezone.utc)
    model.version = 1
    fh, filename = serialize_model(model)

    client.put_object(Bucket='models', Key='rfi_mask/current.alias',
                      ContentType='text/plain',
                      Body=b'config/sandbox_v1.alias\n')
    client.put_object(Bucket='models', Key='rfi_mask/config/sandbox_v1.alias',
                      ContentType='text/plain',
                      Body=f'../fixed/{filename}\n')
    client.put_object(Bucket='models',
                      ContentType='application/x-hdf5',
                      Key=f'rfi_mask/fixed/{filename}', Body=fh)


def put_band_mask(client):
    band_mask_table = astropy.table.Table(BAND_MASK)
    model = katsdpmodels.band_mask.BandMaskRanges(band_mask_table)
    model.author = 'Sandbox setup script'
    model.comment = 'Band mask model for use in sandbox testing'
    model.target = 'MeerKAT, all bands'
    model.created = datetime(2020, 6, 26, 10, 51, tzinfo=timezone.utc)
    model.version = 1
    fh, filename = serialize_model(model)

    for band in ['l', 's', 'u', 'x']:
        for ratio in [1, 8, 16]:
            target = f'{band}/nb_ratio={ratio}'
            client.put_object(Bucket='models',
                              Key=f'band_mask/current/{target}.alias',
                              ContentType='text/plain',
                              Body=f'../../config/{target}/sandbox_v1.alias\n')
            client.put_object(Bucket='models',
                              Key=f'band_mask/config/{target}/sandbox_v1.alias',
                              ContentType='text/plain',
                              Body=f'../../../fixed/{filename}\n')
    client.put_object(Bucket='models',
                      Key=f'band_mask/fixed/{filename}',
                      ContentType='application/x-hdf5',
                      Body=fh)


def main():
    session = botocore.session.get_session()
    config = botocore.config.Config(s3={'addressing_style': 'path'})
    client = session.create_client(
        's3',
        endpoint_url='http://localhost:9000/',
        use_ssl=False,
        aws_access_key_id='minioaccesskey',
        aws_secret_access_key='miniosecretkey',
        config=config
    )
    try:
        client.create_bucket(Bucket='models')
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    client.put_bucket_policy(Bucket='models', Policy=json.dumps(BUCKET_POLICY))

    put_rfi_mask(client)
    put_band_mask(client)


if __name__ == '__main__':
    main()
