#!/usr/bin/env nextflow

/*
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

process BUILD_DB {
  label 'db'
  tag "${date}"

  input:
  path vcv_xml
  path rcv_xml
  val date

  output:
  path "clinvar_${date}.sqlite", emit: db
  path 'xml_md5.json', emit: md5

  script:
  def db = "clinvar_${date}.sqlite"

  """
  rm -f ${db}
  build_db.py ${vcv_xml} ${db}
  build_rcv_db.py ${rcv_xml} ${db}
  build_annotations.py ${db}

  python3 - <<'PY'
  import hashlib, json

  def md5(path):
      h = hashlib.md5()
      with open(path, "rb") as f:
          for block in iter(lambda: f.read(1 << 20), b""):
              h.update(block)
      return h.hexdigest()

  with open("xml_md5.json", "w") as out:
      json.dump({"vcv_xml": md5("${vcv_xml}"), "rcv_xml": md5("${rcv_xml}")}, out)
  PY
  """
}
