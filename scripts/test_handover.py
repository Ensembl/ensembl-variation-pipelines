#!/usr/bin/env python3

# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import requests
import sys
import os
import json
import argparse
from glob import glob

SITE_URL = {
    "live": "https://beta.ensembl.org",
    "staging": "https://staging-2020.ensembl.org"
}

def parser_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload_json", dest="payload_json", help="path to handover payload json file - either for api or tracks")
    parser.add_argument("--site", dest="site", choices=["live", "staging"], default="live")
    parser.add_argument("--type", dest="type", choices=["tracks", "api"], default="tracks")
    parser.add_argument("--quiet", dest="quiet", action="store_true", help="Only report errors")
    
    return parser.parse_args(args)

def fetch_data(url):
    """Fetch JSON data from a URL using HTTP GET.

    Args:
        url (str): URL to request.

    Returns:
        dict|list|None: Parsed JSON response if the request and parsing succeed,
            otherwise None on any RequestException.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def check_tracks_api_loading(track_payload, site, quiet=False):
    print("Checking loading of track api ...")

    track_cat_url = SITE_URL[site] + "/api/tracks/track_categories/"
    track_url = SITE_URL[site] + "/api/tracks/track/"

    error_msg = {}
    with open(track_payload, "r") as f:
        track_metadata = json.load(f)
        for genome_uuid in track_metadata:
            label = track_metadata[genome_uuid]["label"]
            description = track_metadata[genome_uuid]["description"]
            bigbed = os.path.basename(
                track_metadata[genome_uuid]["datafiles"]["details"]
            )
            bigwig = os.path.basename(
                track_metadata[genome_uuid]["datafiles"]["summary"]
            )

            error_msg[genome_uuid] = {
                "msgs": [],
                "tracks": {}
            }
            url = f"{track_cat_url}/{genome_uuid}"
            data = fetch_data(url)
            if data:
                for category in data["track_categories"]:
                    if category["label"] != "Short variants by resource":
                        continue

                    if category["track_category_id"] != "short-variants":
                        msg = "\tCategory id -\n"
                        msg += "\t     GOT: {category['track_category_id']}\n"
                        msg += "\tEXPECTED: short-variants\n"
                        error_msg[genome_uuid]['msgs'].append(msg)

                    if category["type"] != "Variation":
                        msg = f"\tCategory type -\n"
                        msg += "\t     GOT: {category['type']}\n"
                        msg += "\tEXPECTED: Variation\n"
                        error_msg[genome_uuid]['msgs'].append(msg)

                    if len(category["track_list"]) > 1:
                        msg = f"\tNumber of tracks -\n"
                        msg += f"\t     GOT: {len(category['track_list'])} tracks\n"
                        msg += f"\tEXPECTED: 1 track\n"
                        error_msg[genome_uuid]['msgs'].append(msg)

                    for track in category["track_list"]:
                        error_msg[genome_uuid]["tracks"][track['track_id']] = []
                        if track["type"] != "variant":
                            msg = f"\tTrack type -\n"
                            msg += f"\t     GOT: {track['type']}\n"
                            msg += f"\tEXPECTED: variant\n"
                            error_msg[genome_uuid]["tracks"][track['track_id']].append(msg)

                        if track["label"] != label:
                            msg = f"\tTrack label -\n"
                            msg += f"\t     GOT: {track['label']}\n"
                            msg += f"\tEXPECTED: {label}\n"
                            error_msg[genome_uuid]["tracks"][track['track_id']].append(msg)

                        if track["description"] != description:
                            msg = f"\tTrack description -\n"
                            msg += f"\t     GOT: {track['description']}\n"
                            msg += f"\tEXPECTED: {description}\n"
                            error_msg[genome_uuid]["tracks"][track['track_id']].append(msg)

                    url = f"{track_url}/{track['track_id']}"
                    data = fetch_data(url)
                    if data:
                        if data["label"] != label:
                            msg = f"\t(/track endpoint) Track label-\n"
                            msg += f"\t     GOT: {data['label']}\n"
                            msg += f"\tEXPECTED: {label}\n"
                            error_msg[genome_uuid]["tracks"][track['track_id']]

                        if data["datafiles"]["variant-details"] != bigbed:
                            msg = "\tbiBed file -\n"
                            msg += f"\t     GOT: {data['datafiles']['variant-details']}\n"
                            msg += f"\tEXPECTED: {bigbed}\n"
                            error_msg[genome_uuid]["tracks"][track['track_id']]

                        if data["datafiles"]["variant-summary"] != bigwig:
                            msg = "\tbiWig file-\n"
                            msg += f"\t     GOT: {data['datafiles']['variant-summary']}\n"
                            msg += f"\tEXPECTED: {bigwig}\n"
                            error_msg[genome_uuid]["tracks"][track['track_id']]
                    else:
                        msg = "No track information found."
                        error_msg[genome_uuid]["tracks"][track['track_id']].append(msg)
            else:
                msg = "No track category information found."
                error_msg[genome_uuid]['msgs'].append(msg)

    for genome_uuid in error_msg:
        any_error = False
        if error_msg[genome_uuid]["msgs"]:
            any_error = True
        for track in error_msg[genome_uuid]["tracks"]:
            if error_msg[genome_uuid]["tracks"][track]:
                any_error = True
        
        if any_error:
            print(f"genome: {genome_uuid}")
            for msg in error_msg[genome_uuid]["msgs"]:
                print(msg)
                any_error = True
            for track in error_msg[genome_uuid]["tracks"]:
                print(f"track: {track}")
                for msg in error_msg[genome_uuid]["tracks"][track]:
                    print(msg)
                    any_error = True
        if not any_error and not quiet:
            print(f"genome: {genome_uuid}")
            print("All OK.\n")

def check_track_files_copy(track_payload, site, quiet=False):
    print("Checking copy of track files ...")

    target_dir = os.environ.get('GB_DIR', None)
    if target_dir is None:
        raise EnvironmentError("Set GB_DIR to where genome browser files are located and try again...")
    
    if site == "live" and "/staging/" in target_dir:
        print(f"live site selected, updating gb directory...")
        target_dir = target_dir.replace("staging", "live")
    if site == "staging" and "/live/" in target_dir:
        print(f"staging site selected, updating gb directory...")
        target_dir = target_dir.replace("live", "staging")

    error_msg = {}
    with open(track_payload, "r") as f:
        track_metadata = json.load(f)
        for genome_uuid in track_metadata:
            any_error = False
            error_msg[genome_uuid] = []
            datafiles = track_metadata[genome_uuid]['datafiles']
            variant_files = glob(target_dir + f"/{genome_uuid}/variant*")

            for type in datafiles:
                file_name = os.path.basename(datafiles[type])
                target_file_path = os.path.join(target_dir, genome_uuid, file_name)

                if not os.path.isfile(target_file_path):
                    any_error = True
                    error_msg[genome_uuid].append(f"\tNOT FOUND: {file_name}")

            
            if len(variant_files) > len(datafiles):
                any_error = True

                variant_filenames = [os.path.basename(file) for file in variant_files]
                data_filenames = [os.path.basename(datafiles[type]) for type in datafiles]
                excess_filenames = set(variant_filenames) - set(data_filenames)

                error_msg[genome_uuid].append("\n\tExcess variant track files in gb directory -")
                for file in excess_filenames:
                    error_msg[genome_uuid].append(f"\t{ file }")

            if any_error:
                print(f"genome: {genome_uuid}")
                for msg in error_msg[genome_uuid]:
                    print(msg)
            if not any_error and not quiet:
                print(f"genome: {genome_uuid}")
                print("All OK.")

def check_api_files_copy(api_payload, site, quiet=False):
    print("Checking copy of api files ...")

    target_dir = os.environ.get('GB_DIR', None)
    if target_dir is None:
        raise EnvironmentError("Set GB_DIR to where genome browser files are located and try again...")
    
    if site == "live" and "/staging/" in target_dir:
        print(f"live site selected, updating gb directory...")
        target_dir = target_dir.replace("staging", "live")
    if site == "staging" and "/live/" in target_dir:
        print(f"staging site selected, updating gb directory...")
        target_dir = target_dir.replace("live", "staging")

    error_msg = {}
    with open(api_payload, "r") as f:
        api_metadata = json.load(f)
        for dataset in api_metadata:
            any_error = False
            genome_uuid = dataset['genome_uuid']
            error_msg[genome_uuid] = []
            variant_files = glob(target_dir + f"/{genome_uuid}/variation.vcf.gz*")

            file_name = "variation.vcf.gz"
            target_file_path = os.path.join(target_dir, genome_uuid, file_name)
            if not os.path.isfile(target_file_path):
                any_error = True
                error_msg[genome_uuid].append(f"\tNOT FOUND: {file_name}")

            if len(variant_files) > 2:
                any_error = True
                error_msg[genome_uuid].append(f"\n\tExcess files; file count {len(variant_files)}, expected 2.")

            if any_error:
                print(f"genome: {genome_uuid}")
                for msg in error_msg[genome_uuid]:
                    print(msg)
            if not any_error and not quiet:
                print(f"genome: {genome_uuid}")
                print("All OK.")

def main(args=None):
    """Validate track API endpoints against a local track metadata file.

    Reads a track metadata JSON file, queries the remote /track_categories and /track
    API endpoints for each genome UUID and compares returned values (label, description,
    datafile names) with the expected values from the metadata file. Prints mismatches.

    Args:
        argv (list): Command-line argument vector; argv[1] should be the path to the
            track metadata JSON file.

    Raises:
        FileNotFoundError: If the provided track metadata file does not exist.
    """

    args = parser_args(args)
    payload_json = args.payload_json
    site = args.site
    type = args.type
    quiet = args.quiet

    if not os.path.isfile(payload_json):
        raise FileNotFoundError(
            f"No such file - {payload_json}, please provide correct payload file"
        )
    
    if type == "tracks":
        check_tracks_api_loading(payload_json, site, quiet)
        check_track_files_copy(payload_json, site, quiet)
    if type == "api":
        check_api_files_copy(payload_json, site, quiet)

if __name__ == "__main__":
    sys.exit(main())
