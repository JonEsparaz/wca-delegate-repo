import argparse
import csv
import json
import random
import requests
from collections import defaultdict, deque
import re
from typing import Dict, List, Set

from events import EVENT_IDS

def filter_concurrent_groups(group: str, groups: Set[str]) -> List[str]:
    group_number = group.split(" ")[-1]
    non_concurrent_group_names = []
    concurrent_group_names = []

    for g in groups:
        if g.endswith(group_number):
            concurrent_group_names.append(g)
        else:
            non_concurrent_group_names.append(g)

    return non_concurrent_group_names, concurrent_group_names

def init_in_memory_group_representations(csv_file_path):
    '''
    Read an input groups CSV file and returns in-memory representations of the groups to
    assist in creating judging and scrambling assignments.

    Parameters:
        `csv_file_path`: Path to your input CSV file

    Returns (as an ordered tuple):
        `competitors_to_judging_assignments` (dict): A dictionary of dictionaries that maps competitor (name) to event ID to judging assignment (initialized as empty string)
        `competitors_to_scrambling_assignments` (dict): A dictionary of dictionaries that maps competitor (name) to event ID to scrambling assignment (initialized as empty string)
        `event_to_groups_to_competitors` (dict): A dictionary of dictionaries of deques that maps event ID to group (e.g., Red 1) to competitor (name)
    '''
    competitors_to_judging_assignments = defaultdict(lambda: defaultdict(str))
    competitors_to_scrambling_assignments = defaultdict(lambda: defaultdict(str))
    event_to_groups_to_competitors = defaultdict(lambda: defaultdict(deque))

    with open(csv_file_path, 'r') as f:
        groups_dict = csv.DictReader(f)

        for competitor_groups in groups_dict:
            for event_id, group in competitor_groups.items():
                if event_id in EVENT_IDS:
                    competitors_to_judging_assignments[competitor_groups['name']][event_id] = ''
                    competitors_to_scrambling_assignments[competitor_groups['name']][event_id] = ''
                    if len(group) > 0: # ignore empty CSV columns
                        event_to_groups_to_competitors[event_id][group].append(competitor_groups['name'])

    shuffle_competitors_in_group(event_to_groups_to_competitors)

    return competitors_to_judging_assignments, competitors_to_scrambling_assignments, event_to_groups_to_competitors

def shuffle_competitors_in_group(event_to_groups_to_competitors: Dict):
    for groups in event_to_groups_to_competitors.values():
        for competitors in groups.values():
            random.shuffle(competitors)

def assign_role_to_group(
        group: str,
        other_group_names: List[str] = [],
        all_groups: Dict = None, 
        assignments: Dict = None,
        event: str = None,
        delegates: Set[str] = None, 
        approved_scramblers: Set[str] = None,
        max_scramble_assignments_per_competitor = 1):
    for other_group in other_group_names:
        if len(all_groups[other_group]) == 0:
            continue
        person = all_groups[other_group][-1]
        all_groups[other_group].pop()
        if approved_scramblers is not None:
            num_assignments_already_given = len(list(filter(lambda g: len(g) > 0, assignments[person].values())))
            if person in approved_scramblers and num_assignments_already_given < max_scramble_assignments_per_competitor:
                assignments[person][event] = group
                return True
        elif person not in delegates:
            assignments[person][event] = group
            return True
    return False

def assign_scramblers_and_judges(
        judges_per_group = 0, judging_assignments = None,
        scramblers_per_group = 0, scrambling_assignments = None, 
        delegates = None, approved_scramblers = None, 
        max_scramble_assignments_per_competitor = 1,
        experienced_competitors = None):
    '''Takes two dictionaries and assigns judges and scramblers accordingly. Return by parameter.'''
    for event, groups in event_to_groups_to_competitors.items():
        for group in groups.keys():
            non_concurrent_group_names, concurrent_group_names = filter_concurrent_groups(group, groups)
            
            judges_assigned_to_group = 0
            scramblers_assigned_to_group = 0

            common_params = {
                'other_group_names': non_concurrent_group_names,
                'all_groups': groups,
                'event': event,
                'max_scramble_assignments_per_competitor': max_scramble_assignments_per_competitor,
                'delegates': delegates,
            }

            scramblers_available = True
            while scramblers_assigned_to_group < scramblers_per_group:
                if scramblers_available and assign_role_to_group(group, approved_scramblers = approved_scramblers, assignments = scrambling_assignments, **common_params):
                    scramblers_assigned_to_group += 1
                else:
                    print(f'WARN cannot assign enough scrambler(s) for {event}, {group}. Consider manual Delegate assignments.')
                    scramblers_available = False
                    break

            initial_num_experienced_competitors = len(experienced_competitors)
            experienced_competitor_attempts = 0

            judges_available_from_other_groups_available = True
            while judges_assigned_to_group < judges_per_group:
                if judges_available_from_other_groups_available and assign_role_to_group(group, assignments = judging_assignments, **common_params):
                    judges_assigned_to_group += 1
                else:
                    judges_available_from_other_groups_available = False
                    person = experienced_competitors[0]
                    is_available = True
                    for concurrent_group in concurrent_group_names:
                        if person in groups[concurrent_group]:
                            is_available = False
                            break
                    if judging_assignments[person][event] or scrambling_assignments[person][event]:
                        is_available = False
                    if is_available:
                        experienced_competitors.pop()
                        judging_assignments[person][event] = group
                        judges_assigned_to_group += 1
                    experienced_competitor_attempts += 1
                    if experienced_competitor_attempts > initial_num_experienced_competitors:
                        print(f'WARN cannot assign enough judges(s) for {event}, {group}. Consider manual Delegate assignments.')
                        break

def make_output_csv_files(csv_fieldnames = [], judging_assignments = None, scrambling_assignments = None, delegates = None):
    index_to_assignment_type = ['judge', 'scramble']
    for index, assignments in enumerate((judging_assignments, scrambling_assignments)):
        with open(f'_{index_to_assignment_type[index]}_out_{args.input}', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            for competitor, competitor_assignments in assignments.items():
                if len(list(filter(lambda x: x != '', competitor_assignments.values()))) > 0:
                    competitor_assignments['name'] = competitor
                    writer.writerow(competitor_assignments)
                elif index_to_assignment_type[index] == 'judge' and competitor not in delegates:
                    print(f'WARN {competitor} has 0 judging assignments. Consider a manual assignment.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True, type=str, help='Full path to input CSV file.')
    parser.add_argument('-c', '--competition', required=True, type=str, help='Competition ID, e.g., PickeringC2023')
    args = parser.parse_args()

    if not args.input.endswith(".csv"):
        raise ValueError("--input is not a CSV file")

    wcif_id_regex = re.compile('20[0-9]{2}$')
    if not wcif_id_regex.search(args.competition):
        raise ValueError("--competition is not a WCIF competition ID")

    with open('config.json', 'r') as f:
        config = json.load(f)

    judges_per_group = config.get('judgesPerGroup', 0)
    scramblers_per_group = config.get('scramblersPerGroup', 0)
    max_scramble_assignments_per_competitor = config.get('maxScrambleAssignmentsPerCompetitor', 1)
    approved_scramblers = set(config.get('scramblers', []))
    experienced_competitor_starting_year = config.get('experiencedCompetitorStartingYear', 2020)

    competitors_to_judging_assignments, competitors_to_scrambling_assignments, event_to_groups_to_competitors = init_in_memory_group_representations(args.input)
    
    wcif_response = requests.get(f'https://www.worldcubeassociation.org/api/v0/competitions/{args.competition}/wcif/public')
    wcif_response_json = wcif_response.json()
    delegates = set(map(lambda p: p['name'], filter(lambda p: 'delegate' in p['roles'] or 'trainee-delegate' in p['roles'], wcif_response_json['persons'])))
    name_to_wca_id = { person['name'].split(' (')[0]: person['wcaId'] for person in wcif_response_json['persons'] }

    experienced_competitors = []
    for competitor in competitors_to_judging_assignments.keys():
        if competitor in delegates:
            continue
        wca_id: str = name_to_wca_id.get(competitor)
        if wca_id is not None and len(wca_id) > 0 and int(wca_id[0:4]) <= experienced_competitor_starting_year:
           experienced_competitors.append(competitor)

    random.shuffle(experienced_competitors)

    assign_scramblers_and_judges(
        judges_per_group = judges_per_group,
        judging_assignments = competitors_to_judging_assignments, 
        scramblers_per_group = scramblers_per_group,
        scrambling_assignments = competitors_to_scrambling_assignments,
        delegates = delegates,
        approved_scramblers = approved_scramblers,
        max_scramble_assignments_per_competitor = max_scramble_assignments_per_competitor,
        experienced_competitors = experienced_competitors
    )

    csv_fieldnames = ['name', *event_to_groups_to_competitors.keys()]
    make_output_csv_files(
        csv_fieldnames = csv_fieldnames, 
        judging_assignments = competitors_to_judging_assignments, 
        scrambling_assignments = competitors_to_scrambling_assignments,
        delegates = delegates
    )
