from subprocess import Popen, PIPE, STDOUT
import queue
import threading
import logging

def worker(file_name, todo_queue, done_queue):
    with Popen(["magma", "-b", file_name], stdin = PIPE, stdout = PIPE, stderr = PIPE) as w:
        while True:
            item = todo_queue.get()
            # Note: item shall not contain newlines!
            w.stdin.write((item + "\n").encode('utf-8'))
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
    # items = []
    q = queue.Queue()
    with Popen(["magma", "-b", file_name], stdin = PIPE, stdout = PIPE, stderr = PIPE) as p:
        while True:
            try:
                line = p.stdout.readline()
                if line == b'':
                    break
                # items.append(line.decode('utf-8').strip())
                q.put(line.decode('utf-8').strip())
            except:
                break
    return q

def merge_items(file_name, done_queue):
    with Popen(["magma", "-b", file_name], stdin = PIPE, stdout = PIPE, stderr = PIPE) as w:
        for item in list(done_queue.queue):
            # Note: item shall not contain newlines!
            w.stdin.write((item + "\n").encode('utf-8'))
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
    # TODO: parse args for process.m, worker.m, merge.m, debuglevel
    logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.DEBUG,
        datefmt='%Y/%m/%d %H:%M:%S')
    todo = collect_items("p.m")
    done = queue.Queue()
    logging.info(f"{todo.qsize()} items to process")
    logging.debug(list(todo.queue))
    total_work = todo.qsize()

    def queue_status():
        current = todo.qsize()
        logging.info(f"Todo status: {current} items remaining ({(1 - (current / total_work)):0.2f}% done)")
        if current == 0:
            logging.info(f"Todo status: Wrapping up")
        else:
            threading.Timer(1, queue_status).start()

    threading.Timer(1, queue_status).start()
    for _ in range(4):
        threading.Thread(target=worker, args=("w.m", todo, done), daemon=True).start()

    todo.join()
    logging.info('All work done, now merging.')
    logging.debug(list(done.queue))
    merge_items("m.m", done)
    logging.info('Merge done.')

