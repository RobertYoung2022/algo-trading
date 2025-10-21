"""
🧪 Test Data Loading - Validate Bitstamp/Coinbase/Yahoo Format Handling
Test that universal_tester.py can load data from all three sources correctly
"""

import sys
sys.path.append('/Users/bobbyyo/Projects/algo-fun')
sys.path.append('/Users/bobbyyo/Projects/algo-fun/strategies/testing')

from strategies.testing.universal_tester import UniversalStrategyTester

def test_data_loading():
    """Test loading data from all three sources"""

    print("🧪 Testing Data Loading from All Sources")
    print("=" * 70)

    # Initialize tester
    tester = UniversalStrategyTester()

    # Test files for each source
    test_files = [
        {
            'name': 'Bitstamp ETH Daily',
            'path': '/Users/bobbyyo/Projects/algo-fun/dataset_files/crypto_hist_data/Bitstamp_ETHUSD_d.csv',
            'expected_order': 'ascending',  # After fix
            'expected_cols': ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
        },
        {
            'name': 'Coinbase ETH Daily',
            'path': '/Users/bobbyyo/Projects/algo-fun/dataset_files/coinbase/ETHUSD-1d-1000wks-enhanced-data.csv',
            'expected_order': 'ascending',
            'expected_cols': ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
        },
        {
            'name': 'Yahoo ETH 20yr',
            'path': '/Users/bobbyyo/Projects/algo-fun/dataset_files/yahoo/ETHUSD-20yr-yahoo-data.csv',
            'expected_order': 'ascending',
            'expected_cols': ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
        }
    ]

    results = []

    for test_file in test_files:
        print(f"\n{'='*70}")
        print(f"📊 Testing: {test_file['name']}")
        print(f"{'='*70}")

        try:
            # Load data
            df, quality_score = tester._load_and_validate_data(test_file['path'])

            if df is None:
                print(f"❌ FAILED to load {test_file['name']}")
                results.append({'name': test_file['name'], 'status': 'FAILED', 'reason': 'Load returned None'})
                continue

            # Check columns
            missing_cols = [col for col in ['Open', 'High', 'Low', 'Close'] if col not in df.columns]
            if missing_cols:
                print(f"❌ Missing required columns: {missing_cols}")
                results.append({'name': test_file['name'], 'status': 'FAILED', 'reason': f'Missing columns: {missing_cols}'})
                continue

            # Check data order (ascending by index)
            if len(df) > 1:
                first_date = df.index[0]
                last_date = df.index[-1]

                if first_date < last_date:
                    print(f"✅ Data order: ASCENDING (correct)")
                    print(f"   First date: {first_date}")
                    print(f"   Last date:  {last_date}")
                else:
                    print(f"❌ Data order: DESCENDING (incorrect)")
                    print(f"   First date: {first_date}")
                    print(f"   Last date:  {last_date}")
                    results.append({'name': test_file['name'], 'status': 'FAILED', 'reason': 'Wrong date order'})
                    continue

            # Show sample data
            print(f"\n📈 First 3 rows:")
            print(df.head(3))
            print(f"\n📉 Last 3 rows:")
            print(df.tail(3))

            # Check for volume
            has_volume = 'Volume' in df.columns
            print(f"\n{'✅' if has_volume else '⚠️'} Volume column: {'Present' if has_volume else 'Missing'}")

            # Summary
            print(f"\n✅ SUCCESS - {test_file['name']}")
            print(f"   Rows: {len(df)}")
            print(f"   Columns: {list(df.columns)}")
            print(f"   Quality Score: {quality_score}")

            results.append({
                'name': test_file['name'],
                'status': 'PASSED',
                'rows': len(df),
                'quality': quality_score
            })

        except Exception as e:
            print(f"❌ EXCEPTION loading {test_file['name']}: {e}")
            results.append({'name': test_file['name'], 'status': 'FAILED', 'reason': str(e)})

    # Summary
    print(f"\n{'='*70}")
    print(f"📊 Test Results Summary")
    print(f"{'='*70}\n")

    for result in results:
        status_icon = '✅' if result['status'] == 'PASSED' else '❌'
        print(f"{status_icon} {result['name']}: {result['status']}")
        if result['status'] == 'FAILED':
            print(f"   Reason: {result.get('reason', 'Unknown')}")
        else:
            print(f"   Rows: {result['rows']}, Quality: {result['quality']}")

    # Overall result
    passed = sum(1 for r in results if r['status'] == 'PASSED')
    total = len(results)

    print(f"\n{'='*70}")
    if passed == total:
        print(f"🎉 ALL TESTS PASSED ({passed}/{total})")
        print(f"✅ Data loading working correctly for all sources!")
    else:
        print(f"⚠️ SOME TESTS FAILED ({passed}/{total} passed)")
        print(f"❌ Data loading needs fixes")
    print(f"{'='*70}")

    return passed == total


if __name__ == '__main__':
    success = test_data_loading()
    sys.exit(0 if success else 1)
