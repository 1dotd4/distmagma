# distmagma - parallel computation glue for Magma Computer Algebra
# Copyright (C) 2025 d4 <coding@unpx.net>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from subprocess import Popen, PIPE, STDOUT
import queue
import threading
import logging

def worker(file_name, progressive, todo_queue, done_queue):
    with Popen(["magma", "-b", file_name], stdin = PIPE, stdout = PIPE, stderr = PIPE) as w:
        # Wait for library loading
        while True:
            l = w.stdout.readline().decode('utf-8').strip()
            if l == "READY":
                break
        # Tell the process its name for logging purposes
        w.stdin.write((str(progressive) + "\n").encode('utf-8'))
        w.stdin.flush()
        while True:
            item = todo_queue.get()
            # Note: purge newlines in item
            w.stdin.write((item.replace("\n", " ")  + "\n").encode('utf-8'))
            w.stdin.flush()
            # Process output
            out = ""
            while True:
                line_out = w.stdout.readline()
                # Note: worker shall output a separated newline
                #       to signal end of output, otherwise hangs.
                if line_out == b'\n':
                    break
                out += line_out.decode('utf-8').strip()
            done_queue.put(out)
            todo_queue.task_done()

def collect_items(file_name):
    q = queue.Queue()
    with Popen(["magma", "-b", file_name], stdin = PIPE, stdout = PIPE, stderr = PIPE) as p:
        # Wait for library loading
        while True:
            l = p.stdout.readline().decode('utf-8').strip()
            if l == "READY":
                break
        while True:
            try:
                line = p.stdout.readline()
                if line == b'':
                    break
                q.put(line.decode('utf-8').strip())
            except:
                break
    return q

def merge_items(file_name, done_queue):
    with Popen(["magma", "-b", file_name], stdin = PIPE, stdout = PIPE, stderr = PIPE) as w:
        # Wait for library loading
        while True:
            l = w.stdout.readline().decode('utf-8').strip()
            if l == "READY":
                break
        for item in list(done_queue.queue):
            # Note: purge newlines in item
            w.stdin.write((item.replace("\n", " ")  + "\n").encode('utf-8'))
            w.stdin.flush()
        w.stdin.write("false\n".encode('utf-8'))
        w.stdin.flush()

        while True:
            # Waits for magma to save data
            line = w.stdout.readline()
            if line == b'':
                break
            logging.info(line.decode('utf-8').strip())

if __name__ == '__main__':
    # TODO: parse args for maxcpu, process.m, worker.m, merge.m, debuglevel
    maxcpu = 4
    timer_duration = 15 * 60 # 15 minutes

    logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.DEBUG,
        datefmt='%Y/%m/%d %H:%M:%S')
    logging.info("Starting collection")
    todo = collect_items("p.m")
    done = queue.Queue()
    logging.info(f"{todo.qsize()} items to process")
    logging.debug(list(todo.queue))
    total_work = todo.qsize()

    t = None

    def queue_status():
        current = done.qsize()
        logging.info(f"Todo status: {current} items remaining ({(current / total_work):0.2f}% done)")
        if current == 0:
            logging.info(f"Todo status: Wrapping up")
        else:
            t = threading.Timer(timer_duration, queue_status)
            t.start()

    t = threading.Timer(timer_duration, queue_status)
    t.start()
    for progressive in range(maxcpu):
        threading.Thread(target=worker, args=("w.m", progressive, todo, done), daemon=True).start()

    todo.join()
    if t.is_alive():
        t.cancel()
    logging.info('All work done, now merging.')
    logging.debug(list(done.queue))
    merge_items("m.m", done)
    logging.info('Merge done.')

