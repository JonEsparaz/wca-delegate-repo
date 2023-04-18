import argparse
import csv

from events import EVENT_IDS

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True, type=str, help='Full path to input CSV file.')
    parser.add_argument('-j', '--judge', required=True, type=str, help='Full path to judge assignment CSV file.')
    parser.add_argument('-s', '--scramble', required=True, type=str, help='Full path to scramble assignment CSV file.')
    args = parser.parse_args()

    with open(args.judge, 'r') as f:
        judge_dict = csv.DictReader(f)
        competitor_to_judge_assignments = { x['name']: x for x in judge_dict }

    with open(args.scramble, 'r') as f:
        scramble_dict = csv.DictReader(f)
        competitor_to_scramble_assignments = { x['name']: x for x in scramble_dict }

    with open(args.input, 'r') as f:
        groups_dict = csv.DictReader(f)
        for competitor in groups_dict:
            name = competitor['name']
            judging_assignments = competitor_to_judge_assignments.get(name, {})
            scramble_assignments = competitor_to_scramble_assignments.get(name, {})
            
            for event_id, group in competitor.items():
                if event_id in EVENT_IDS:
                    compete = group.split(' ')[-1]
                    judge = judging_assignments.get(event_id, '').split(' ')[-1]
                    scramble = scramble_assignments.get(event_id, '').split(' ')[-1]
                    compete = compete if len(compete) > 0 else 'not competing'
                    judge = judge if len(judge) > 0 else 'not judging'
                    scramble = scramble if len(scramble) > 0 else 'not scrambling'

                    assert compete != judge != scramble, f'{name}\tcompete: {compete}\tjudge: {judge}\tscramble: {scramble}'
