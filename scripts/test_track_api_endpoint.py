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


def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def main(argv):
    track_metadata_file = argv[1]
    if not os.path.isfile(track_metadata_file):
        raise FileNotFoundError(
            f"No such file - {track_metadata_file}, please provide correct track metadata file"
        )

    with open(track_metadata_file, "r") as f:
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

            print(f"Genome UUID: {genome_uuid}")
            url = f"https://staging-2020.ensembl.org/api/tracks/track_categories/{genome_uuid}"
            data = fetch_data(url)
            if data:
                for category in data["track_categories"]:
                    if category["label"] != "Short variants by resource":
                        continue

                    print("from /track_categories endpoint")
                    if category["track_category_id"] != "short-variants":
                        print(f"Category id: {category['track_category_id']}")
                        print("\tEXPECTED: short-variants")
                    if category["type"] != "Variation":
                        print(f"Category type: {category['type']}")
                        print("\tEXPECTED: Variation")

                    if len(category["track_list"]) > 1:
                        print(
                            f"EXPECTED: single tracks, but {len(category['track_list'])} tracks found"
                        )

                    for track in category["track_list"]:
                        print(f"Track id: {track['track_id']}")
                        if track["type"] != "variant":
                            print(f"Track type: {track['type']}")
                            print("\tEXPECTED: variant")

                        if track["label"] != label:
                            print(f"Track label: {track['label']}")
                            print(f"\tEXPECTED: {label}")

                        if track["description"] != description:
                            print(f"Track description: {track['description']}")
                            print(f"\tEXPECTED: {description}")

                    url = f"https://staging-2020.ensembl.org/api/tracks/track/{track['track_id']}"
                    data = fetch_data(url)

                    if data:
                        print("from /track endpoint:")
                        if data["label"] != label:
                            print(f"label: {data['label']}")
                            print(f"\tEXPECTED: {label}")
                        if data["datafiles"]["variant-details"] != bigbed:
                            print(f"biBed file: {data['datafiles']['variant-details']}")
                            print(f"\tEXPECTED: {bigbed}")
                        if data["datafiles"]["variant-summary"] != bigwig:
                            print(f"biWig file: {data['datafiles']['variant-summary']}")
                            print(f"\tEXPECTED: {bigwig}")
                    else:
                        print("No track categories retrieved.")

            else:
                print("No track information retrieved.")
            print()


if __name__ == "__main__":
    main(sys.argv)
