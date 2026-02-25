#!/usr/bin/env python3
import os
import sys

# ensure src is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from services.bloomberg_tools import create_session, close_session, bdh, bdp

from logger_config import DATA_DIR
from src.parsers import inav_bskt


def main():
    path = os.path.join(DATA_DIR, 'Harvest_INAVBSKT_ALL.20260218.CSV')
    path = os.path.abspath(path)
    print('Testing inav_bskt on', path)

    reader = inav_bskt(path)
    blocks = reader.extract()

    print('Blocks found:', len(blocks))
    if not blocks:
        print('No blocks parsed.')
        return
    #print the full dictionary and the first.5 rows of the holdings for the first block
    for i, b in enumerate(blocks[:2]):
        print(f'--- Block {i} metadata:')
        for k, v in b['metadata'].items():
            print(f'    {k}: {v}')
        try:
            df = b['holdings']
            print(f'--- Block {i} holdings shape: {df.shape}')
            print(df.head(50))
        except Exception as e:
            print('--- Block', i, 'holdings: could not print DataFrame:', e)
    print(type(blocks[0]['metadata']['TRADE_DATE']))
    print(type(blocks[1]['metadata']['CREATION_UNIT_SIZE']))



if __name__ == '__main__':
    main()
    session = create_session()
    try:
        hist = bdh(session, ["XIU CN Equity"], ["PX_LAST"], "20260101", "20260220")
        snap = bdp(session, ["XIU CN Equity"], ["PX_LAST", "VOLUME"])
    finally:
        close_session(session)