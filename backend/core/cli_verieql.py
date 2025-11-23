# -*- coding: utf-8 -*-


import argparse
import time
from multiprocessing import (
    Process,
    Queue,
    cpu_count,
    Manager,
)

import tqdm
import ujson

from constants import *
from environment import Environment
from errors import *
from logger import LOGGER
from utils import (
    divide,
)

parser = argparse.ArgumentParser(description='Graphiti cli using VeriEQL as backend')
parser.add_argument('-f', '--file', type=str)
parser.add_argument('-s', '--bound_size', type=int, default=999999999)
parser.add_argument('-t', '--timeout', type=int, default=TIMEOUT)
# multiprocessing might decrease the CPU's performance on each core, but it decreases the total time cost
parser.add_argument('-c', '--num_cores', type=int, default=cpu_count())
parser.add_argument('-i', '--integrity_constraint', default=1, choices=[0, 1], type=int)
parser.add_argument('-o', '--out_file', type=str, default=None)
# config
parser.add_argument('-l', '--label', type=str, default="human")
parser.add_argument('-u', '--use_optimization', type=str, default=1)

args = parser.parse_args()


def verify(schema, constraint, query1, query2, bound_size, graph, queue: Queue):
    err_info = None
    with Environment(timer=True, generate_code=True, graph=graph, out_file=None) as env:
        try:
            NON_DEL_TUPLES = {}
            frozen = {}
            for idx in reversed(range(len(constraint))):
                if 'frozen' in constraint[idx]:
                    frozen[constraint[idx]['frozen'][0]['value']] = constraint[idx]['frozen'][1]
                    constraint.pop(idx)
            for name, db in schema.items():
                NON_DEL_TUPLES |= env.create_database(db, bound_size=bound_size, name=name, frozen=frozen)
            # for name, db in schema.items():
            #     env.create_database(db, bound_size=bound_size, name=name)
            if args.integrity_constraint and constraint is not None:
                env.add_constraints(constraint, NON_DEL_TUPLES)
            env.save_checkpoints()
            env.reload_checkpoints()
            result = env.analyze(query1, query2)
            if result == False:
                raise NotEquivalenceError()
            else:
                state = STATE.EQUIV
        except SyntaxError as err:
            err_info = str(err)
            state = STATE.SYN_ERR
        except NotEquivalenceError as err:
            err_info = str(err)
            state = STATE.NON_EQUIV
        except TimeoutError as err:
            err_info = str(err)
            state = STATE.TIMEOUT
        except NotSupportedError as err:
            err_info = str(err)
            state = STATE.NOT_SUP_ERR
        except UnknownError as err:
            err_info = str(err)
            state = STATE.UNKNOWN
        except NotImplementedError as err:
            err_info = str(err)
            state = STATE.NOT_IMPL_ERR
        except Exception as err:
            err_info = str(err)
            state = STATE.OTHER_ERR
        if env.counterexample is not None and env.cypher_counterexample is not None:
            counterexample = env.counterexample + '\n' + env.cypher_counterexample
        else:
            counterexample = None
        if env.solving_time is None:
            outs = [state, round(time.time() - env.traversing_time, 6), None, counterexample, err_info]
        else:
            outs = [state, env.traversing_time, env.solving_time, counterexample, err_info]
        for o in outs:
            queue.put(o)


def process_ends_with_max_timeout(
        index, schema, constraint, query1, query2, max_bound_size, graph, states, time_cost,
        timeout, queue: Queue
):
    result = {
        'index': index,
        'pair': [query1, query2],
        'states': [],
        'times': [],
        'counterexample': None,
        'err': None,
    }
    if states is not None and time_cost is not None:
        result['states'] = states
        result['times'] = time_cost

    pbar = tqdm.tqdm(total=max_bound_size, desc=f'Bound size: {0:5d} | Thread: {1:3d}', )

    bound_size = len(result['states']) + 1
    pbar.set_description(f'Bound size: {bound_size:5d} | Thread: {1:3d}', refresh=False)
    pbar.update(bound_size)
    queue.empty()
    proc = Process(
        target=verify,
        args=(schema, constraint, query1, query2, bound_size, graph, queue,),
    )
    proc.start()

    start = time.time()
    while time.time() - start <= timeout:
        if not proc.is_alive():
            # All the processes are done, break now.
            try:
                state, traversing_time, solving_time, counterexample, err = [queue.get() for _ in range(queue.qsize())]
                result['states'].append(state)
                result['times'].append([traversing_time, solving_time])
                result['counterexample'] = counterexample
                result['err'] = err
            except ValueError:
                # out of memory
                state = STATE.OOM
                result['states'].append(state)
                result['times'].append(None)

            if state == STATE.EQUIV:
                # only continute if queries are = or !=
                bound_size = len(result['states']) + 1
                pbar.set_description(f'Bound size: {bound_size:5d} | Thread: {1:3d}', refresh=False)
                pbar.update(bound_size)
                queue.empty()
                proc = Process(
                    target=verify,
                    args=(schema, constraint, query1, query2, bound_size, graph, queue,),
                )
                proc.start()
            else:
                # not support
                break
        else:
            time.sleep(0.1)  # Just to avoid hogging the CPU
    else:
        # We only enter this if we didn't 'break' above.
        LOGGER.debug("timed out, killing all processes")
        proc.terminate()
        proc.join()
        result['states'].append(STATE.TIMEOUT)
        result['times'].append(None)
    return result


def core(pbar, out_file, desc, timeout, worker_idx):
    pbar = tqdm.tqdm(pbar, desc=desc, mininterval=10)

    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, 'w') as writer:
        for parameters in pbar:
            manager = Manager()
            queue = manager.Queue()
            out = process_ends_with_max_timeout(*parameters, timeout, queue)
            # to log for check
            out['schema'] = parameters[1]
            out['constraint'] = parameters[2]
            print(ujson.dumps(out, ensure_ascii=False), file=writer)


def encoding(line, label, opt=False, **config):
    __replace = lambda x: x.replace('.', '__').replace('$', '_')

    def formulate_constraints(constraints):
        out = []
        for pks in constraints['primary']:
            out.append({"primary": [{"value": __replace(pk)} for pk in pks]})
        for fks in constraints['foreign']:
            out.append({"foreign": [{"value": __replace(fk)} for fk in fks]})
        if 'others' in constraints:
            for other in constraints['others']:
                operator = str.lower(list(other.keys())[0])
                operands = other[operator]

                match operator:
                    case 'primary' | 'unique':
                        out.append({"primary": [{'value': __replace(opd)} for opd in operands]})
                    case 'neq':
                        out.append({"neq": [
                            {'value': __replace(opd)} if isinstance(opd, str) else opd
                            for opd in operands
                        ]})
                    case 'in':
                        out.append({'in': [[{'value': __replace(opd)} for opd in opds] for opds in operands]})
                    case 'in_const':
                        out.append({'in_const': [{'value': __replace(operands[0])}, operands[1]]})
                    case 'alias_label':
                        out.append({'alias_label': [{'value': opd} for opd in operands]})
                    case 'not_null':
                        out.append({'not_null': {'value': __replace(operands)}})
                    case 'inc':
                        out.append({'inc': {'value': __replace(operands)}})
                    case 'subset':
                        out.append({'subset': [{'value': opd} for opd in operands]})
                    case 'frozen':
                        out.append({'frozen': [{'value': __replace(operands[0])}, operands[1]]})
                    case 'consistof':
                        out.append({"consistof": []})
                    case 'mapsto':
                        out.append({'mapsto': [{'value': __replace(opd)} for opd in operands]})
                    case 'between':
                        out.append({'between': [{'value': __replace(operands[0])}] + operands[1:]})
                    case 'gte' | 'gt' | 'lt' | 'lte':
                        out.append({operator: [
                            {'value': __replace(opd)} if isinstance(opd, str) else opd
                            for opd in operands
                        ]})
                    case 'eq_bound':
                        conds = []
                        for attr1, attr2 in operands["conds"]:
                            conds.append([{'value': __replace(attr1)}, {'value': __replace(attr2)}])
                        other['eq_bound']["conds"] = conds
                        out.append(other)
                    case 'eq':
                        out.append({'mapsto': [{'value': __replace(opd)} for opd in operands]})
                    case 'imply':
                        out.append({'imply': [
                            {'eq': [{'value': __replace(opd)} for opd in operands[0]['eq']]},
                            {'eq': [{'value': __replace(opd)} for opd in operands[1]['eq']]}
                        ]})
                    case _:
                        raise NotSupportedError(operator)
        return out

    sql1 = line['relation']['sql']
    schema1 = line['relation']['schema']
    constraint1 = formulate_constraints(line['relation']['constraint'])

    sql2 = line[label]['translation']['opt_sql' if opt else 'sql']
    if sql2 is not None:
        sql2 = sql2.replace('$', '_')
    schema2 = {__replace(k): v for k, v in line[label]['translation']['schema'].items()}
    constraint2 = formulate_constraints(line[label]['translation']['constraint'])

    transformer_constraint = []
    for sql_attr, cypher_attr in line['graph']['db_transformer']:
        sql_attr = __replace(sql_attr)
        cypher_attr = __replace(cypher_attr)
        transformer_constraint.append({'mapsto': [{'value': cypher_attr}, {'value': sql_attr}]})
    # N$MOVIE.BADMOVIE => N$MOVIE.ID in BADMOVIE.ID
    if 'row_del' in line['graph'].get('auxiliary', {}):
        transformer_constraint.append(
            {'row_del': [{'value': __replace(attr)} for attr in line['graph']['auxiliary']['row_del']]})
    if 'consistof' in line['graph'].get('auxiliary', {}):
        const = line['graph']['auxiliary']['consistof']
        transformer_constraint.append({
            "consistof": [
                __replace(const[0]),
                [__replace(table) for table in const[1]],
                {"eq": [__replace(const[2]['eq'][0]), const[2]['eq'][1]]},
                {"map": [[__replace(attr) for attr in const[3]['map'][0]],
                         [__replace(attr) for attr in const[3]['map'][1]]]},
                {"mapsto": [
                    [__replace(attr) for attr in const[4]['mapsto'][0]],
                    [__replace(attr) for attr in const[4]['mapsto'][1]],
                ]}
            ]
        })

    # generate_code: generate SQL code and running outputs if it finds a counterexample
    # timer: show time costs
    # show_counterexample: print counterexample?
    schema = schema1 | schema2
    constraints = constraint1 + constraint2 + transformer_constraint
    cypher = line[label]['cypher']
    return schema, constraints, sql1, sql2, cypher


def train(args):
    with open(args.file, 'r') as reader:
        parameters = []
        config = {'generate_code': True, 'timer': True, 'show_counterexample': True, 'out_file': None}
        for idx, line in enumerate(reader, start=1):
            line = ujson.loads(line)
            # for label, opt in [["human", 1], ["chatgpt", 1], ["chatgpt4", 1]]:
            schema, constraint, sql1, sql2, cypher = encoding(line, args.label, args.use_optimization, **config)
            graph = {'schema': line['graph']['schema'],
                     'links': {tuple(k): v for k, v in line['graph']['links']},
                     'cypher': cypher}
            states = timecost = None
            parameters.append([idx, schema, constraint, sql1, sql2, args.bound_size, graph, states, timecost])
        count = len(parameters)

        # args.num_cores = 1
        # parameters = [parameters[2]]

        if args.num_cores == 1:
            core(
                parameters,
                args.out_file,
                f'Bound size: {args.bound_size:3d} | Thread: {1:3d}',
                args.timeout,
                worker_idx=1,
            )
        else:
            parameters = list(divide(parameters, partitions=args.num_cores))
            procs = []
            for worker_idx in range(len(parameters)):
                proc = Process(
                    target=core,
                    args=(
                        parameters[worker_idx],
                        args.out_file + str(worker_idx),
                        f'Bound size: {args.bound_size:3d} | Thread: {worker_idx:3d}',
                        args.timeout,
                        worker_idx,
                    ),
                )
                proc.start()
                procs.append(proc)

            for proc in procs:
                proc.join()

            with open(args.out_file, 'w') as writer:
                results = []
                for worker_idx in range(len(parameters)):
                    file = args.out_file + str(worker_idx)
                    with open(file, 'r') as reader:
                        for line in reader:
                            line = ujson.loads(line)
                            results.append(line)
                    os.remove(file)
                assert len(results) == count, (args.file, len(results), count)
                for line in results:
                    print(ujson.dumps(line), file=writer)


if __name__ == '__main__':
    # args.label = "human"
    # args.label = "chatgpt4o"
    args.file = f'benchmarks/{args.label}.jsonlines'
    args.out_file = f'benchmarks/{args.label}.verieql'
    train(args)
