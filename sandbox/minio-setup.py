#!/usr/bin/env python3

from datetime import datetime, timezone
import hashlib
import io
import json
from urllib.parse import urlsplit, urljoin

import numpy as np
import astropy.units as u
import astropy.table
import botocore.session
import requests

import katsdpmodels.band_mask
import katsdpmodels.rfi_mask
import katsdpmodels.primary_beam


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


def copy_primary_beams(client):
    BANDS = 'lu'    # The only bands implemented so far - update as more are added
    urls = {
        f'https://sdpmodels.kat.ac.za/primary_beam/current/{group}/{antenna}/{band}.alias'
        for group in ['individual', 'cohort']
        for antenna in [f'm{i:03d}' for i in range(64)]
        for band in BANDS
    }
    urls.update({
        f'https://sdpmodels.kat.ac.za/primary_beam/current/cohort/{cohort}/{band}.alias'
        for cohort in ['meerkat']
        for band in BANDS
    })
    done = set()
    with requests.Session() as session:
        while urls:
            url = urls.pop()
            done.add(url)
            with session.get(url, timeout=20) as resp:
                resp.raise_for_status()
                client.put_object(Bucket='models',
                                  ContentType=resp.headers['Content-type'],
                                  Key=urlsplit(url)[1:],
                                  Body=resp.data)
                if url.endswith('.alias'):
                    rel_path = resp.text.strip()
                    rel_parts = urlsplit(rel_path)
                    assert not rel_parts.scheme and not rel_parts.netloc
                    new_url = urljoin(resp.url, rel_path)
                    if new_url not in done:
                        urls.add(new_url)


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
    copy_primary_beams(client)


if __name__ == '__main__':
    main()
