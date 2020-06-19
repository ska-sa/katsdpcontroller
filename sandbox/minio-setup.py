#!/usr/bin/env python3

from datetime import datetime, timezone
import hashlib
import io
import json

import numpy as np
import h5py
import astropy.units as u
import astropy.table
import botocore.session

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


def write_rfi_model(hdf5: h5py.File, model: katsdpmodels.rfi_mask.RFIMaskRanges):
    # TODO: move this into katsdpmodels. It'll need to be more generic to
    # ensure that it gets the units right and handles None attributes.
    hdf5.attrs['model_type'] = 'rfi_mask'
    hdf5.attrs['model_format'] = 'ranges'
    hdf5.attrs['model_comment'] = model.comment
    hdf5.attrs['model_author'] = model.author
    hdf5.attrs['model_created'] = model.created.isoformat()
    hdf5.attrs['model_target'] = model.target
    hdf5.attrs['model_version'] = model.version
    array = model.ranges.as_array(names=('min_frequency', 'max_frequency', 'max_baseline'))
    hdf5.create_dataset('ranges', data=array)


def main():
    # Prepare data
    rfi_mask_table = astropy.table.QTable(RFI_MASK)
    rfi_mask_table['min_frequency'] <<= u.Hz
    rfi_mask_table['max_frequency'] <<= u.Hz
    rfi_mask_table['max_baseline'] <<= u.m
    model = katsdpmodels.rfi_mask.RFIMaskRanges(rfi_mask_table)
    model.author = 'Sandbox setup script'
    model.comment = 'RFI mask model for use in sandbox testing'
    model.target = 'MeerKAT'
    model.created = datetime(2020, 6, 15, 14, 11, tzinfo=timezone.utc)
    model.version = 1
    fh = io.BytesIO()
    with h5py.File(fh, 'w') as hdf5:
        write_rfi_model(hdf5, model)
    checksum = hashlib.sha256(fh.getvalue()).hexdigest()

    # Load it to minio
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
    client.put_object(Bucket='models', Key='rfi_mask/current.alias',
                      ContentType='text/plain',
                      Body=b'config/v1.alias\n')
    client.put_object(Bucket='models', Key='rfi_mask/config/v1.alias',
                      ContentType='text/plain',
                      Body=f'../hash/sha256_{checksum}.hdf5\n'.encode())
    fh.seek(0)
    client.put_object(Bucket='models',
                      ContentType='application/x-hdf5',
                      Key=f'rfi_mask/hash/sha256_{checksum}.hdf5', Body=fh)


if __name__ == '__main__':
    main()
