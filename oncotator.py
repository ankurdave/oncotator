#!/usr/bin/env python2.7

import itertools
import requests
import sys

if len(sys.argv) < 3:
    print """Usage: oncotator.py INPUT_FILE OUTPUT_FILE"""
    sys.exit(1)

form_url = 'http://www.broadinstitute.org/oncotator/'
chunk_size_lines = 7500

input_file = sys.argv[1]
output_file = sys.argv[2]

# From http://docs.python.org/2/library/itertools.html#recipes
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

is_first_chunk = True
chunk_index = 1

with open(input_file, 'r') as f:
  with open(output_file, 'w') as out:
    for chunk in grouper(f, chunk_size_lines):
      # Strip None entries from the chunked lines and concatenate
      chunk_lines = [line for line in chunk if line is not None]
      chunk_string = ''.join(chunk_lines)

      sys.stdout.write('Converting chunk %d (%d lines, %d chars)...' % (
        chunk_index, len(chunk_lines), len(chunk_string)))
      sys.stdout.flush()
      chunk_index += 1

      # Get the web form and extract its CSRF token
      form = requests.get(form_url)
      csrf_token = form.cookies['csrftoken']
      sys.stdout.write('.')
      sys.stdout.flush()

      # Submit the current chunk and include the CSRF token
      data = {
        'data': chunk_string,
        'paste_submit': 'Submit',
        'csrfmiddlewaretoken': csrf_token
      }
      cookies = {'csrftoken': csrf_token}
      headers = {'Referer': form_url}
      r = requests.post(form_url, data=data, cookies=cookies, headers=headers)
      sys.stdout.write('.')
      sys.stdout.flush()

      # Get the resulting output
      download_url = unicode.replace(r.url, '/report/', '/download/')
      output_chunk = requests.get(download_url).text
      sys.stdout.write('.')
      sys.stdout.flush()

      # Strip the first two header lines for all but the first chunk
      if is_first_chunk:
        is_first_chunk = False
      else:
        output_chunk = output_chunk.split('\n', 2)[2]

      # Append the result to the output file
      out.write(output_chunk.encode('UTF-8'))
      sys.stdout.write('done.\n')
      sys.stdout.flush()
