# -*- coding:utf-8 -*-
import functools
import gc
import itertools
import operator as Operator
import pprint
from collections import defaultdict
from time import time
from typing import *

from ordered_set import OrderedSet
from z3 import (
    DeclareSort,
    StringSort,
    BoolSort,
    IntSort,
    Const,
    Solver,
    Function,
    sat,
    unknown,
)

import utils
from code_snippet import CodeSnippet
from constants import *
from errors import (
    UnknownError,
    NotSupportedError,
    NotEquivalenceError,
)
from formulas.columns import (
    FAttribute,
)
from formulas.expressions import (
    FSymbol,
    FExpression,
    FCast,
    FVarchar,
    FTime,
    FTimestamp,
    FExpressionTuple
)
from formulas.tables import (
    FBaseTable,
    FOrderByTable,
)
from formulas.tuples import (
    FBaseTuple,
    FField,
)
from logger import LOGGER
from scope import Scope
from sql_parser import SQLParser
from verifiers import (
    Verifier,
    BagSemanticsVerifier,
    ListSemanticsVerifier,
)
from visitors.interm_function import IntermFunc
from visitors.visitor import Visitor
from writers.script import Script

UN_SUPPORTED_CAST_TYPE = FVarchar | FTime | FTimestamp


class Environment:
    # for verifiers and aggregation functions to write code in files
    Tuple1 = 'tuple1'
    Tuple2 = 'tuple2'

    TupleSort = DeclareSort('TupleSort', ctx=Z3_CONTEXT)
    BooleanSort = BoolSort(Z3_CONTEXT)
    VarSort = IntSort(Z3_CONTEXT)
    StringSort = StringSort(Z3_CONTEXT)

    DELETED_FUNCTION = Function('DELETED', TupleSort, BooleanSort)
    NULL = Function('NULL', TupleSort, StringSort, BooleanSort)
    COUNT_FUNCTION = Function('COUNT', TupleSort, StringSort, VarSort)
    MAX_FUNCTION = Function('MAX', TupleSort, StringSort, VarSort)
    MIN_FUNCTION = Function('MIN', TupleSort, StringSort, VarSort)
    AVG_FUNCTION = Function('AVG', TupleSort, StringSort, VarSort)
    SUM_FUNCTION = Function('SUM', TupleSort, StringSort, VarSort)

    def __init__(self, generate_code=False, semantics=None, timer=False, show_counterexample=False, **kwargs):
        if generate_code and kwargs.get('out_file', None) is not None:
            self._script_writer = Script()
        else:
            self._script_writer = None
        self.sql_code = {'tables': {}, 'sql1': None, 'sql2': None}
        self.show_counterexample = show_counterexample
        self.counterexample = None
        if semantics == 'bag':
            self.verifier = BagSemanticsVerifier(self)
        elif semantics == 'list':
            self.verifier = ListSemanticsVerifier(self)
        else:
            self.verifier = Verifier(self)
        self.traversing_time = time() if timer else None
        self.encoding_times = []
        self.solving_time = None
        self.solver = Solver(ctx=Z3_CONTEXT)
        self.visitor = Visitor(self)
        self.symbolic_count = 1

        self.attributes = {}
        self.variables = {}
        self.functions = {}
        self.COUNT_ALL_FUNCTION, self.COUNT_ALL_NULL_FUNCTION = self._define_COUNT_ALL()
        self.tuples = {}
        self.tuple_sorts = {}
        self.databases = {}
        self.base_databases = {}
        self._database_num = 1
        self.DBMS_facts = []
        self.sql_parser = SQLParser()
        self.checkpoints = {
            'databases': OrderedSet(),
            'tuples': OrderedSet(),
            'tuple_sorts': OrderedSet(),
            'attributes': OrderedSet(),
            'variables': OrderedSet(),
            'functions': OrderedSet(),
        }
        # only store the last OrderBy results, cuz intermediate OrderBy does not matter
        self.orderby_constraints = []
        # # only store uninterpreted functions of the outermost queries
        # self.uninterpreted_functions = {}

        # works for MAX/MIN
        self.bound_constraints = set()
        self.kwargs = kwargs

        # to cypher
        self.to_cypher = self.kwargs.get('graph', None) is not None
        if self.to_cypher:
            self.num_node = 1
            self.num_edge = 1
            self.cypher_counterexample = []

    def _define_COUNT_ALL(self):
        all_value = Const(f"COUNT_ALL__{self.StringSort}", self.StringSort)
        self.COUNT_ALL_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.COUNT_FUNCTION(x, all_value),
            description=f'{self.COUNT_FUNCTION}(?, {all_value})'
        )
        self.COUNT_ALL_NULL_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.NULL(x, all_value),
            description=f'{self.NULL}(?, {all_value})'
        )
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f'{all_value} = Const(f"COUNT_ALL__{self.StringSort}", __{self.StringSort})',
                    docstring=f'define `COUNT(*)`',
                )
            )
        return self.COUNT_ALL_FUNCTION, self.COUNT_ALL_NULL_FUNCTION

    def __enter__(self):
        LOGGER.debug(
            '#--------------------------------------- CREATING ENVIRONMENT ---------------------------------------#\n\n')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tuples.clear()
        self.functions.clear()
        self.databases.clear()
        self.tuple_sorts.clear()
        self.variables.clear()
        self.attributes.clear()
        del self.sql_parser
        del self.solver
        del self.visitor
        gc.collect()

        LOGGER.debug(
            '#--------------------------------------- ENVIRONMENT ENDED ---------------------------------------#\n\n')

    def __str__(self):
        return f'{self.__class__.__name__}(id={id(self)})'

    def __repr__(self):
        return self.__str__()

    ############################ Register tuple and table ############################

    def _get_new_tuple_name(self) -> str:
        return f't{len(self.tuple_sorts) + 1}'

    def _get_new_databases_name(self) -> str:
        return f'Table{self._database_num}'

    def register_variable(self, name, variable, is_string=False):
        if name not in self.variables:
            if is_string:
                self.checkpoints['variables'].add(name)
            self.variables[name] = variable
            return True
        else:
            LOGGER.debug(f'Variable {name} has been already registered in Environment.')
            return False

    def register_attribute(self, name, attribute):
        if name not in self.attributes:
            # self.attributes[name] = self._declare_function(attribute)  # call z3 function
            self.attributes[name] = attribute  # call z3 function
            return True
        else:
            LOGGER.debug(f'Attribute {name} has been already registered in Environment.')
            return False

    def register_tuple_sort(self, name, tuple):
        if name not in self.tuple_sorts:
            self.tuple_sorts[name] = tuple
            return True
        else:
            LOGGER.debug(f'Tuple {name} has been already registered in Environment.')
            return False

    def register_tuple(self, name, tuple):
        if name not in self.tuples:
            self.tuples[name] = tuple
            if self._script_writer is not None:
                self._script_writer.tuples.append(name)
            return True
        else:
            LOGGER.debug(f'BaseTuple {name} has been already registered in Environment.')
            return False

    def register_function(self, name, function):
        if name not in self.functions:
            self.functions[name] = function
            return True
        else:
            LOGGER.debug(f'Function {name} has been already registered in Environment.')
            return False

    def register_database(self, name, database):
        if name not in self.databases:
            self.databases[name] = database
            self._database_num += 1
            return True
        else:
            LOGGER.debug(f'Database {name} has been already registered in Environment.')
            return False

    ############################ declare new sorts here ############################

    def _declare_tuple_sort(self, name):
        """
        defined z3 tuple sort
        """
        tuple = Const(name, self.TupleSort)
        self.register_tuple_sort(name, tuple)
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f"{name} = Const('{name}', __{self.TupleSort})",
                    docstring=f'define a tuple `{tuple}`',
                )
            )
        return tuple

    def _get_new_tuple_sort(self) -> str:
        new_tuple = f't{len(self.tuple_sorts) + 1}'
        return self._declare_tuple_sort(new_tuple)

    def _declare_variable(self, attribute: FAttribute, sort=None):
        if sort is None:
            sort = self.StringSort
        value = self._declare_value(attribute, sort)
        attribute.__STRING_SORT__ = value  # for NULL function
        self.register_variable(utils.__pos_hash__(attribute), value,
                               is_string=isinstance(attribute, FSymbol) and isinstance(attribute.value, str))

    def _declare_value(self, attribute: [FAttribute or FSymbol], sort=None, register=False):
        """
        declare symbolic values
        """
        # RunError: sort or self.VarSort
        if sort is None:
            sort = self.VarSort
        value = Const(f"{attribute}__{sort}", sort)
        self.register_variable(utils.__pos_hash__(attribute), value,
                               is_string=isinstance(attribute, FSymbol) and isinstance(attribute.value, str))
        if register:
            # to register literal variable,
            # e.g., CLERK__Int = Const('CLERK__Int', __Int) and CLERK__Int == hash("CLERK")
            self.DBMS_facts.append(value == IntVal(str(utils.__pos_hash__(attribute))))
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f"{value} = Const('{attribute}__{sort}', __{sort})",
                    docstring=f'define `{value}` for NULL function',
                )
            )
        return value

    def _declare_max_attribute(self, attribute: FAttribute, sort=None):
        """
        declare a StringSort MAX for Aggregation function - MAX
        """
        if sort is None:
            sort = self.StringSort
        value = Const(f"MAX_{attribute}__{sort}", sort)
        attribute.____MAX_STRING_SORT____ = value
        attribute.MAX_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.MAX_FUNCTION(x, value),
            description=f'MAX({attribute})',
        )
        attribute.MAX_NULL_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.NULL(x, attribute.__MAX_STRING_SORT__),
            description=f'{self.NULL}({attribute.__MAX_STRING_SORT__})',
        )
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f'{value} = Const(f"MAX_{attribute}__{sort}", __{sort})',
                    docstring=f'define `MAX` variable of {attribute}',
                )
            )
        return value

    def _declare_min_attribute(self, attribute: FAttribute | FExpression, sort=None):
        """
        declare a StringSort MIN for Aggregation function - MIN
        """
        if sort is None:
            sort = self.StringSort
        value = Const(f"MIN_{attribute}__{sort}", sort)
        attribute.__MIN_STRING_SORT__ = value
        attribute.MIN_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.MIN_FUNCTION(x, value),
            description=f'MIN({attribute})',
        )
        attribute.MIN_NULL_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.NULL(x, attribute.__MIN_STRING_SORT__),
            description=f'{self.NULL}({attribute.__MIN_STRING_SORT__})',
        )
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f'{value} = Const(f"MIN_{attribute}__{sort}", __{sort})',
                    docstring=f'define `MIN` variable of {attribute}',
                )
            )
        return value

    def _declare_count_attribute(self, attribute: FAttribute, sort=None):
        """
        declare a StringSort COUNT for Aggregation function - COUNT
        """
        if sort is None:
            sort = self.StringSort
        value = Const(f"COUNT_{attribute}__{sort}", sort)
        attribute.__COUNT_STRING_SORT__ = value
        attribute.COUNT_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.COUNT_FUNCTION(x, value),
            description=f'{self.COUNT_FUNCTION}(?, {value})'
        )
        attribute.COUNT_NULL_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.NULL(x, attribute.__COUNT_STRING_SORT__),
            description=f'{self.NULL}(?, {attribute.__COUNT_STRING_SORT__})'
        )
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f'{value} = Const(f"COUNT_{attribute}__{sort}", __{sort})',
                    docstring=f'define `COUNT` variable of {attribute}',
                )
            )
        return value

    def _declare_avg_attribute(self, attribute: FAttribute, sort=None):
        """
        declare a StringSort AVG for Aggregation function - AVG
        """
        if sort is None:
            sort = self.StringSort
        value = Const(f"AVG_{attribute}__{sort}", sort)
        # self.register_avg(utils.__pos_hash__(attribute), value)
        attribute.__AVG_STRING_SORT__ = value
        attribute.AVG_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.AVG_FUNCTION(x, value),
            description=f'{self.AVG_FUNCTION}(?, {value})'
        )
        attribute.AVG_NULL_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.NULL(x, attribute.__AVG_STRING_SORT__),
            description=f'{self.NULL}(?, {attribute.__AVG_STRING_SORT__})'
        )
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f'{value} = Const(f"AVG_{attribute}__{sort}", __{sort})',
                    docstring=f'define `AVG` variable of {attribute}',
                )
            )
        return value

    def _declare_sum_attribute(self, attribute: FAttribute, sort=None):
        """
        declare a StringSort SUM for Aggregation function - SUM
        """
        if sort is None:
            sort = self.StringSort
        value = Const(f"SUM_{attribute}__{sort}", sort)
        # self.register_sum(utils.__pos_hash__(attribute), value)
        attribute.__SUM_STRING_SORT__ = value
        attribute.SUM_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.SUM_FUNCTION(x, value),
            description=f'{self.SUM_FUNCTION}(?, {value})'
        )
        attribute.SUM_NULL_FUNCTION = IntermFunc(
            z3_function=lambda x, **kwargs: self.NULL(x, attribute.__SUM_STRING_SORT__),
            description=f'{self.NULL}(?, {attribute.__SUM_STRING_SORT__})'
        )
        if self._script_writer is not None:
            self._script_writer.variable_declaration.append(
                CodeSnippet(
                    code=f'{value} = Const(f"SUM_{attribute}__{sort}", __{sort})',
                    docstring=f'define `SUM` variable of {attribute}',
                )
            )
        return value

    def declare_attribute(self, name: str, literal: str, _uuid=None):
        """
        declare an attribute/column of databases
        """
        attribute = FAttribute(self, prefix=str.upper(name), literal=str.upper(literal), _uuid=_uuid)
        # to register a String Sort to verify NULL
        self._declare_variable(attribute)
        attribute.NULL = IntermFunc(
            z3_function=lambda x, **kwargs: self.NULL(x, attribute.__STRING_SORT__),
            description=f'{self.NULL}(?, {attribute.__STRING_SORT__})',
        )
        self._declare_function(attribute)
        return attribute

    def _declare_function(self, attribute: FAttribute, input_sorts: Sequence = None):
        """
        declare a function to call symbolic values
        Example: EMP_id = Function('EMP.id', T, IntSort())
        """
        if input_sorts is None:
            input_sorts = [self.TupleSort]
        function = Function(str(attribute), *input_sorts, self.VarSort)
        self.register_function(utils.__pos_hash__(attribute), function)
        attribute.VALUE = function
        if self._script_writer is not None:
            self._script_writer.function_declaration.append(
                CodeSnippet(
                    code=f"{attribute} = Function('{attribute}', __TupleSort, __Int)",
                    docstring=f'define `{attribute}` function to retrieve columns of tuples',
                )
            )
        return function

    def _declare_tuple(self, fields: Sequence, name=None, tuple_sort=None):
        """
        declare a tuple and automatically register its z3 variable
        Example: t1 = Const('t1', TupleSort)
        """
        if tuple_sort is None:
            name = name or self._get_new_tuple_name()
            tuple = FBaseTuple(fields, name)
            self.register_tuple(name, tuple)
            tuple.SORT = self._declare_tuple_sort(name)
        else:
            name = str(tuple_sort)
            tuple = FBaseTuple(fields, name)
            tuple.SORT = tuple_sort
        return tuple

    def _declare_table(self, tuples, name=None):
        name = name or self._get_new_databases_name()
        database = FBaseTable(tuples, name)
        self.register_database(name.__str__(), database)
        return database

    def register_base_table(self, table, name):
        self.base_databases[name] = table

    def create_database(
            self,
            attributes: Dict,
            bound_size=2, symbol='x',
            key_attributes: Sequence = None, NULL_ratio: float = 0.0,
            name: str = None,
            **kwargs,
    ):
        NON_DEL_TUPLES = {}
        if self.sql_code is not None:
            self.sql_code['tables'][name] = {}
            for attr, type in attributes.items():
                if type is None:
                    type = 'INTEGER'
                type = str.upper(type)
                if type == 'VARCHAR' or type.startswith('ENUM'):
                    type = 'VARCHAR(20)'
                elif type == 'DATE':
                    pass
                else:
                    type = 'INTEGER'
                self.sql_code['tables'][name][attr] = type

        name = str.upper(name or self._get_new_databases_name())
        key_attributes = key_attributes or set()
        tuples = []
        type_constraints = []
        saved_attributes = {}
        if name in kwargs.get('frozen', {}):
            bound_size = kwargs['frozen'][name]
        for idx in range(bound_size):
            fields = []

            tuple_sort = self._get_new_tuple_sort()
            for i, (attr, type) in enumerate(attributes.items(), start=1):
                if attr in saved_attributes:
                    attribute = saved_attributes[attr]
                else:
                    attribute = self.declare_attribute(name, literal=attr)
                    saved_attributes[attr] = attribute
                if attr not in key_attributes and random.random() < NULL_ratio:
                    value = FSymbol(self.NULL)
                else:
                    value = FSymbol(f'{symbol}{self.symbolic_count}')
                    self.symbolic_count += 1
                value = self._declare_value(str(value))

                if type is not None:
                    upper_type = str.upper(type)
                    match upper_type:
                        case 'BOOLEAN' | 'BOOL':
                            type_constraints.append(Or(
                                attribute.VALUE(tuple_sort) == Z3_1,
                                attribute.VALUE(tuple_sort) == Z3_0,
                            ))
                        case 'DATE':
                            type_constraints.extend([
                                DATE_LOWER_BOUND <= attribute.VALUE(tuple_sort),
                                attribute.VALUE(tuple_sort) <= DATE_UPPER_BOUND,
                            ])
                        case 'INT':
                            type_constraints.extend([
                                INT_LOWER_BOUND <= attribute.VALUE(tuple_sort),
                                attribute.VALUE(tuple_sort) <= INT_UPPER_BOUND,
                            ])
                        case 'VARCHAR':
                            type_constraints.append(INT_UPPER_BOUND < attribute.VALUE(tuple_sort))
                        case _:
                            # 'INT' | 'VARCHAR' | 'TEXT' | ...
                            pass
                fields.append(FField(attribute, value))
            base_tuple = self._declare_tuple(fields, tuple_sort=tuple_sort)
            tuples.append(base_tuple)

            # register DBMS in z3 solver
            # self.DBMS_facts.append(Not(self.DELETED_FUNCTION(base_tuple.SORT)))  # Not(Deleted(tuple))
            NON_DEL_TUPLES[base_tuple.name] = base_tuple.SORT
            for operand in base_tuple:
                # attr(tuple) == ...
                self.DBMS_facts.append(operand.operator.value(operand.attribute.VALUE(base_tuple.SORT), operand.value))
        if len(type_constraints) > 0:
            self.DBMS_facts.extend(type_constraints)
        table = self._declare_table(tuples, name)
        self.register_base_table(table, table.name)
        LOGGER.debug(pprint.pformat(self.databases))
        return NON_DEL_TUPLES

    def add_constraints(self, constraints, NON_DEL_TUPLES, **kwargs):
        if constraints is None:
            return

        @functools.lru_cache()
        def _get_attribute(expr):
            # find attr
            table_name = expr[:expr.find('__')]
            table = self.databases[table_name]
            attribute = None
            for attr in table.attributes:
                if attr == expr:
                    attribute = attr
            values = [FExpressionTuple(attribute.NULL(t.SORT), attribute.VALUE(t.SORT)) for t in table.tuples]
            return values

        def _f(expr):
            if isinstance(expr, int):
                return IntVal(str(expr))
            elif isinstance(expr, float):
                return RealVal(str(expr))
            elif isinstance(expr, str):
                if utils.is_date_format(expr):
                    return utils.strptime_to_int(expr)
                else:
                    return _get_attribute(expr)
            elif isinstance(expr, list):
                return [_f(e) for e in expr]
            elif isinstance(expr, dict):
                operator = list(expr.keys())[0]
                operands = expr[operator]

                match operator:
                    case 'primary' | 'unique':
                        operands = [_f(e) for e in operands]
                        out = []
                        if len(operands) == 1:
                            # primary key is an attribute
                            out.extend([Not(attr.NULL) for attr in operands[0]])
                            for key1, key2 in list(itertools.combinations(operands[0], 2)):
                                out.append(key1.VALUE != key2.VALUE)
                        else:
                            # primary key is a pair
                            out.extend([Not(attr.NULL) for attr in itertools.chain(*operands)])
                            for pairs in list(itertools.combinations(zip(*operands), 2)):
                                out.append(
                                    Not(
                                        And(*[key1.VALUE == key2.VALUE for key1, key2 in zip(*pairs)])
                                    )
                                )
                        out = And(*out)
                        return out
                    case 'foreign':
                        lhs_attrs, rhs_attrs = [_f(e) for e in operands]
                        out = []
                        for lhs_attr in lhs_attrs:
                            tmp = [
                                utils.encode_equality(lhs_attr.NULL, rhs_attr.NULL, lhs_attr.VALUE, rhs_attr.VALUE)
                                for rhs_attr in rhs_attrs
                            ]
                            out.append(Or(*tmp))
                        out = And(*out)
                        return out
                    case 'inc':
                        # cannot be NULL
                        operands = _f(operands)
                        if len(operands) > 1:
                            opd0 = operands[0]
                            out = [opd0.VALUE == Z3_0, Not(opd0.NULL), ]
                            for idx, opd in enumerate(operands[1:], start=1):
                                out.extend([opd.VALUE == IntVal(str(idx)), Not(opd.NULL)])
                            out = And(*out)
                            return out
                        else:
                            return None
                    case 'value':
                        return _get_attribute(operands)
                    case 'literal':
                        return self._declare_value(FSymbol(operands), register=True)
                    case 'boolean':
                        operands = _f(operands)
                        return And(*[Or(attr.VALUE == Z3_1, attr.VALUE == Z3_0) for attr in operands])
                    case 'int':
                        operands = _f(operands)
                        return And(*[
                            And(INT_LOWER_BOUND <= attr.VALUE, attr.VALUE <= INT_UPPER_BOUND) for attr in operands
                        ])
                    case 'varchar':
                        operands = _f(operands)
                        # we only set INT_UPPER_BOUND < attr.VALUE, it is good for order by text
                        return And(*[
                            # Or(attr.VALUE < INT_LOWER_BOUND, INT_UPPER_BOUND < attr.VALUE) for attr in operands
                            INT_UPPER_BOUND < attr.VALUE for attr in operands
                        ])
                    case 'date':
                        attr = _f(operands)
                        if isinstance(attr, int):
                            return attr
                        else:
                            return And(DATE_LOWER_BOUND <= attr[0].VALUE, attr[0].VALUE <= DATE_UPPER_BOUND)
                    case 'lt' | 'lte' | 'gt' | 'gte' | 'eq' | 'neq':
                        left_opd = _f(operands[0])
                        right_opd = _f(operands[1])
                        if not isinstance(left_opd, list):
                            left_opd = [left_opd]
                        if not isinstance(right_opd, list):
                            right_opd = [right_opd]
                        out = []
                        for left_f, right_f in itertools.product(left_opd, right_opd):
                            _op = {
                                'lt': Operator.lt,
                                'lte': Operator.le,
                                'gt': Operator.gt,
                                'gte': Operator.ge,
                                'eq': Operator.eq,
                                'neq': Operator.ne,
                            }[operator]
                            if isinstance(left_f, FExpressionTuple) and isinstance(right_f, ArithRef | int | float):
                                if isinstance(right_f, int):
                                    right_f = IntVal(str(right_f))
                                if isinstance(right_f, float):
                                    right_f = RealVal(str(right_f))
                                e = And(_op(left_f.VALUE, right_f), Not(left_f.NULL))
                            elif isinstance(left_f, ArithRef | int | float) and isinstance(right_f, FExpressionTuple):
                                if isinstance(left_f, int):
                                    left_f = IntVal(str(left_f))
                                if isinstance(left_f, float):
                                    left_f = RealVal(str(left_f))
                                e = And(_op(right_f.VALUE, left_f), Not(right_f.NULL))
                            elif isinstance(left_f, FExpressionTuple) and isinstance(right_f, FExpressionTuple):
                                if operator == 'eq':
                                    e = utils.encode_equality(left_f.NULL, right_f.NULL, left_f.VALUE, right_f.VALUE)
                                elif operator == 'neq':
                                    e = utils.encode_inequality(left_f.NULL, right_f.NULL, left_f.VALUE, right_f.VALUE)
                                else:
                                    e = And(Not(left_f.NULL), Not(right_f.NULL), _op(left_f.VALUE, right_f.VALUE))
                            elif isinstance(left_f, ArithRef | int | float) and \
                                    isinstance(right_f, ArithRef | int | float):
                                if isinstance(left_f, int):
                                    left_f = IntVal(str(left_f))
                                else:
                                    left_f = RealVal(str(left_f))

                                if isinstance(right_f, int):
                                    right_f = IntVal(str(right_f))
                                else:
                                    right_f = RealVal(str(right_f))

                                e = _op(left_f, right_f)
                            else:
                                raise NotImplementedError
                            out.append(e)
                        if len(out) == 1:
                            return out[0]
                        else:
                            return And(*out)
                    case 'or' | 'and':
                        operands = [_f(opd) for opd in operands]
                        return {'or': Or, 'and': And}[operator](*operands)
                    case 'not_null':
                        operands = _f(operands)
                        return And(*[Not(opd.NULL) for opd in operands])
                    case 'in':
                        attributes = _f(operands[0])
                        choices = _f(operands[1])
                        out = [Not(attr.NULL) for attr in attributes]
                        for attr in attributes:
                            tmp = utils.simplify([attr.VALUE == value for value in choices], operator=Or)
                            out.append(tmp)
                        out = utils.simplify(out, operator=And) if len(out) > 1 else out[0]
                        return out
                    case 'between':
                        attributes = _f(operands[0])
                        out = [Not(attr.NULL) for attr in attributes]
                        lower_bound, upper_bound = [_f(value) for value in operands[1:]]
                        for attr in attributes:
                            out.extend([lower_bound <= attr.VALUE, attr.VALUE <= upper_bound])
                        out = utils.simplify(out, operator=And) if len(out) > 1 else out[0]
                        return out
                    case 'mapsto':
                        attributes = [_get_attribute(opd['value']) for opd in operands]
                        out = []
                        for lhs_attr, rhs_attr in zip(*attributes):
                            out.append(utils.encode_same(lhs_attr.NULL, rhs_attr.NULL, lhs_attr.VALUE, rhs_attr.VALUE))
                        out = utils.simplify(out, operator=And) if len(out) > 1 else out[0]
                        return out
                    case 'in_const':
                        attributes = _f(operands[0])
                        choices = []
                        for opd in operands[1]:
                            if isinstance(opd, dict) and 'literal' in opd:
                                symbol = self._declare_value(FSymbol(opd['literal']), register=True)
                            else:
                                symbol = self._declare_value(FSymbol(opd), register=True)
                            choices.append(symbol)
                        out = []
                        for attr, value in zip(attributes, choices):
                            out += [Not(attr.NULL), attr.VALUE == value]
                        out = utils.simplify(out, operator=And) if len(out) > 1 else out[0]
                        return out
                    case 'subset':
                        table = [t.name for t in self.base_databases[operands[1]['value']]]
                        table_attrs = self.base_databases[operands[1]['value']].attributes
                        sub_table = [t.name for t in self.base_databases[operands[0]['value']]]
                        sub_table_attrs = self.base_databases[operands[0]['value']].attributes
                        out = []
                        for t_name, sub_t_name in zip(table, sub_table):
                            t_sort = NON_DEL_TUPLES.pop(t_name)
                            sub_t_sort = NON_DEL_TUPLES.pop(sub_t_name)
                            out.append(Not(self.DELETED_FUNCTION(t_sort)))
                            out.append(
                                Implies(
                                    Not(self.DELETED_FUNCTION(sub_t_sort)),
                                    And(*[
                                        utils.encode_same(attr1.NULL(t_sort), attr2.NULL(sub_t_sort),
                                                          attr1.VALUE(t_sort), attr2.VALUE(sub_t_sort))
                                        for attr1, attr2 in zip(table_attrs, sub_table_attrs)
                                    ]),
                                )
                            )
                        out = utils.simplify(out, operator=And) if len(out) > 1 else out[0]
                        return out
                    case "row_del":
                        # {'row_del: ['N$MOVIE.BADMOVIE', 'BADMOVIE']}
                        attrs = _get_attribute(operands[0]['value'])
                        table = [t.SORT for t in self.base_databases[operands[1]['value']]]
                        out = []
                        for attr, t_sort in zip(attrs, table):
                            out += [
                                Implies(attr.VALUE == Z3_1, Not(self.DELETED_FUNCTION(t_sort))),
                                Implies(attr.VALUE == Z3_0, self.DELETED_FUNCTION(t_sort)),
                            ]
                        out = utils.simplify(out, operator=And) if len(out) > 1 else out[0]
                        return out
                    case 'consistof':
                        # INVOLVEMENT.TITLE = DIRECTED => E$DIRECTED = INVOLVEMENT
                        # src_table = dst_tables
                        src_table = [t.SORT for t in self.base_databases[operands[0]]]
                        dst_tables = {
                            dst_table: [tuple.SORT for tuple in self.base_databases[dst_table]]
                            for dst_table in operands[1]
                        }
                        # pop out tuples which may be deleted
                        for dst_table in operands[1]:
                            for tuple in self.base_databases[dst_table]:
                                if tuple.name in NON_DEL_TUPLES:
                                    NON_DEL_TUPLES.pop(tuple.name)
                        # condition
                        attrs = _get_attribute(operands[2]['eq'][0])
                        values = [IntVal(v) for v in operands[2]['eq'][1]]
                        # register a pair function
                        func_name = "consistof_paired"
                        paired_func = Function(func_name, self.TupleSort, self.TupleSort, self.BooleanSort)
                        self.register_function(utils.__pos_hash__(func_name), paired_func)
                        if self._script_writer is not None:
                            self._script_writer.function_declaration.append(
                                CodeSnippet(
                                    code=f"{func_name} = Function('{func_name}', __TupleSort, __TupleSort, __Boolean)",
                                    docstring=f'define `{func_name}`',
                                )
                            )
                        # pair(t_i, r_j) = 1 iff \forall a \in attrs. t_i.a = r_j.a
                        src_attrs = []
                        for table_attr in operands[3]['map'][0]:
                            for attr in self.base_databases[table_attr.split('__', 1)[0]].attributes:
                                if attr == table_attr:
                                    src_attrs.append(attr)
                                    break
                        dst_attrs = {}
                        for table in operands[1]:
                            dst_attrs[table] = []
                            for table_attr in operands[3]['map'][1]:
                                for attr in self.base_databases[table].attributes:
                                    if attr == table + table_attr[1:]:
                                        dst_attrs[table].append(attr)
                                        break

                        # formulate the `consistsof` constraint
                        out = []
                        dst_tuples = [t for ts in dst_tables.values() for t in ts]
                        dst_names = [k for ks in dst_tables.keys() for k in [ks] * (len(dst_tuples) // len(values))]
                        dst_values = [v for vs in values for v in [vs] * (len(dst_tuples) // len(values))]
                        unique_constraints = []
                        for i, (src_t, attr) in enumerate(zip(src_table, attrs)):
                            for j, (dst_t, dst_v, dst_n) in enumerate(zip(dst_tuples, dst_values, dst_names)):
                                out.append(
                                    Implies(
                                        paired_func(src_t, dst_t),
                                        And(
                                            Not(self.DELETED_FUNCTION(dst_t)),
                                            attr.VALUE == dst_v,
                                            *[
                                                attr1.VALUE(src_t) == attr2.VALUE(dst_t)
                                                for attr1, attr2 in zip(src_attrs, dst_attrs[dst_n])
                                            ]
                                        )
                                    )
                                )
                                out.append(
                                    Implies(Not(paired_func(src_t, dst_t)),
                                            Or(
                                                self.DELETED_FUNCTION(dst_t),
                                                attr.VALUE != dst_v
                                            ))
                                )
                            # for 1 <= j <= n, sum(t_i, r_j) = 1
                            unique_constraints.append(
                                Sum(*[If(paired_func(src_t, dst_t), Z3_1, Z3_0) for dst_t in dst_tuples]) == Z3_1
                            )
                        for dst_t in dst_tuples:
                            # for 1 <= i <= n, sum(t_i, r_j) = 1
                            unique_constraints.append(
                                Sum(*[If(paired_func(src_t, dst_t), Z3_1, Z3_0) for src_t in src_table]) == \
                                If(self.DELETED_FUNCTION(dst_t), Z3_0, Z3_1)
                            )
                        self.DBMS_facts += out

                        out = []
                        for sattr, tattr in zip(*operands[4]['mapsto']):
                            rel = sattr.split("__")[0]
                            sattr = self.databases[rel].attributes[self.databases[rel].attributes.index(sattr)]
                            tattr = self.databases[rel].attributes[self.databases[rel].attributes.index(tattr)]
                            for t in dst_tables[rel]:
                                lhs_attr, rhs_attr = sattr(t), tattr(t)
                                out.append(
                                    utils.encode_same(lhs_attr.NULL, rhs_attr.NULL, lhs_attr.VALUE, rhs_attr.VALUE))
                        out = utils.simplify(out, operator=And) if len(out) > 1 else out[0]
                        self.DBMS_facts.append(out)

                        self.DBMS_facts += unique_constraints
                        return None
                    case "eq_bound":
                        # pass
                        tables, conds = operands["tables"], operands["conds"]
                        tables = [self.databases[table] for table in tables]
                        bound_size = len(tables[0])
                        cond_ids = []
                        for idx, (sattr, tattr) in enumerate(conds):
                            sattr, tattr = sattr['value'], tattr['value']
                            s_rel, _ = sattr.split('__')
                            t_rel, _ = tattr.split('__')
                            cond_ids.append(
                                [tables[idx].attributes.index(sattr), tables[idx + 1].attributes.index(tattr)])
                        out = []
                        for tuples in itertools.product(*tables):
                            # tmp_conds = [Not(self.DELETED_FUNCTION(t.SORT)) for t in tuples] # always true
                            tmp_conds = []
                            for idx, (stuple, ttuple) in enumerate(zip(tuples[:-1], tuples[1:])):
                                sidx, tidx = cond_ids[idx]
                                sattr = stuple.attributes[sidx](stuple.SORT)
                                tattr = ttuple.attributes[tidx](ttuple.SORT)
                                tmp_conds.append(
                                    utils.encode_equality(sattr.NULL, tattr.NULL, sattr.VALUE, tattr.VALUE))
                            out.append(If(And(*tmp_conds), Z3_1, Z3_0))
                        out = Sum(*out) == IntVal(f'{bound_size}')
                        self.DBMS_facts.append(out)
                    case 'imply':
                        tables = []
                        # premise
                        pre_attrs = []
                        for opd in operands[0]['eq']:
                            rel, attr = opd["value"].split("__")
                            tables.append(rel)
                            attrs = self.databases[rel].attributes
                            pre_attrs.append(attrs[attrs.index(attr)])
                        # conclusion
                        post_attrs = []
                        for opd in operands[1]['eq']:
                            rel, attr = opd["value"].split("__")
                            attrs = self.databases[rel].attributes
                            post_attrs.append(attrs[attrs.index(attr)])
                        out = []
                        for t1, t2 in itertools.product(self.databases[tables[0]], self.databases[tables[1]]):
                            pre_attr1, pre_attr2 = pre_attrs[0](t1.SORT), pre_attrs[1](t2.SORT)
                            post_attr1, post_attr2 = post_attrs[0](t1.SORT), post_attrs[1](t2.SORT)
                            out.append(Implies(
                                utils.encode_equality(pre_attr1.NULL, pre_attr2.NULL, pre_attr1.VALUE, pre_attr2.VALUE),
                                utils.encode_equality(post_attr1.NULL, post_attr2.NULL, post_attr1.VALUE,
                                                      post_attr2.VALUE)
                            ))
                        self.DBMS_facts += out
                    case _:
                        raise NotImplementedError(expr)
            else:
                raise NotImplementedError(expr)

        for constraint in constraints:
            # constraint = _f(constraint)
            try:
                constraint = _f(constraint)
            except:
                print(constraint)
                constraint = _f(constraint)
            if constraint is not None:
                self.DBMS_facts.append(constraint)
        for t_name, t_sort in NON_DEL_TUPLES.items():
            self.DBMS_facts.append(Not(self.DELETED_FUNCTION(t_sort)))

    def _get_attribute(self, attr):
        if isinstance(attr, FAttribute):
            return self.attributes.get(utils.__pos_hash__(attr), None)
        else:
            out = []
            for attribute in self.attributes.values():
                if attr == attribute:
                    out.append(attribute)
            if len(out) == 0:
                return None
            elif len(out) == 1:
                return out[0]
            else:
                return out

    def _get_variable(self, value: Any):
        return self.variables.get(utils.__pos_hash__(value), None)

    def _get_function(self, attribute: FAttribute | IntermFunc):
        if isinstance(attribute, IntermFunc):  # Aggregation
            pass
        else:
            return self.functions.get(utils.__pos_hash__(attribute), None)

    def _get_tuple_sort(self, tuple: str):
        return self.tuple_sorts.get(tuple, None)

    ############################ store DBMS schemas for mulit-query comparision ############################

    def save_checkpoints(self):
        for key in self.checkpoints.keys():
            elements = list(getattr(self, key).keys())
            self.checkpoints[key].update(elements)

    def reload_checkpoints(self, keys=None):
        if keys is None:
            keys = self.checkpoints.keys()
            self._database_num = 1 + len(self.checkpoints['databases'])
            self.orderby_constraints = []
        for cp_key in keys:
            cp_value = self.checkpoints[cp_key]
            dict_object = getattr(self, cp_key)
            for key in list(dict_object.keys()):
                if key not in cp_value:
                    dict_object.pop(key)
        self.solver.reset()
        self.verifier.reset()

    ############################ parse SQL query into AST ############################

    def parse_sql_query(self, query: str):
        return self.sql_parser.parse(query)  # .replace('\"', '\'')

    ############################ analyze query ############################

    def is_parsable(self, query):
        try:
            query = self.parse_sql_query(query)
            with Scope(self) as scope:
                table = scope.analyze(query)
                return table is not None
        except:
            pass
        return False

    def analyze(self, *queries, out_file: str = None):
        if self.sql_code is not None:
            queries = list(map(str.upper, queries))
            self.sql_code['sql1'] = queries[0] if queries[0][-1] == ';' else queries[0] + ';'
            self.sql_code['sql2'] = queries[1] if queries[1][-1] == ';' else queries[1] + ';'

        if self._script_writer is not None:
            self._script_writer.reload_checkpoints()
            self._script_writer.query = [query.replace('\n', ' ') for query in queries]

        # 1) parse SQL queries
        query_asts = [self.parse_sql_query(query) for query in queries]

        # 2) analyze queries but do not register formulas into z3 environments,
        # and translate/visit the aforementioned queries formulas into the temporary z3 environment
        def _analyze(query, query_idx):
            with Scope(self, f'SCOPE{query_idx}') as scope:
                # start_time = time()
                ctx = scope.analyze(query)
                # print(f">>> traversing time: {time() - start_time:.4f}")
                # start_time = time()
                tables, result_formulas = scope.visit(ctx)
                # print(f">>> visiting time: {time() - start_time:.4f}")
                # we only consider the outermost orderby clause, otherwise it's too complicated
                # 1) SELECT * FROM XXX ORDER BY XXX
                # 2) SELECT XXX FROM XXX ORDER BY XXX
                if scope.orderby_constraints is not None:
                    last_table = self.databases[list(self.databases)[-1]]
                    last_2nd_table = self.databases[list(self.databases)[-2]]
                    if isinstance(last_table, FOrderByTable):
                        if scope.orderby_constraints['tuples'] == [t.SORT for t in last_table]:
                            self.orderby_constraints.append(scope.orderby_constraints)
                        else:
                            raise Exception
                    elif isinstance(last_2nd_table, FOrderByTable):
                        if scope.orderby_constraints['tuples'] == [t.SORT for t in last_2nd_table]:
                            # scope.orderby_constraints['tuples']
                            self.orderby_constraints.append(scope.orderby_constraints)
                        else:
                            raise Exception
                    else:
                        self.orderby_constraints.append(None)
                else:
                    self.orderby_constraints.append(None)
            return tables, result_formulas

        tables, result_formulas = [], []
        for idx, query in enumerate(query_asts, start=1):
            if self.traversing_time is not None:
                encoding_timer = time()
            table, formulas = _analyze(query, idx)
            tables.append(table)
            result_formulas.append(formulas)
            if self.traversing_time is not None:
                self.encoding_times.append(round(time() - encoding_timer, 4))

        # 3) SQL queries equivalence verification
        result = self.compare(tables, result_formulas)
        if result == -1:
            self.sql_code = "Different #columns"
            print(self.sql_code)
            return result

        # 4) write
        if self._script_writer is not None and out_file is not None:
            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            with open(out_file, 'w') as writer:
                print(self._script_writer.eval(self.databases, tables), file=writer)

        if result == True:
            self.sql_code = None
        elif result == False and self.sql_code is not None:
            tables = '\n'.join(table for table in self.sql_code['tables'].values())
            self.sql_code = f"{tables}\n{self.sql_code['sql1']}\n{self.sql_code['sql2']}"

            if self.show_counterexample:
                print(self.counterexample)
                if self.to_cypher:
                    print(self.cypher_counterexample)

        return result

    def compare(
            self, tables: Sequence, result_formulas, out_file: str = None,
    ) -> bool:
        lhs_tuple = list(tables[0].values())[0]
        rhs_tuple = list(tables[1].values())[0]
        if lhs_tuple.name != 'DELETED_TUPLE' and rhs_tuple.name != 'DELETED_TUPLE' and \
                len(lhs_tuple.attributes) != len(rhs_tuple.attributes):
            return -1
        else:
            for idx, (lhs_attr, rhs_attr) in enumerate(zip(lhs_tuple.attributes, rhs_tuple.attributes)):
                lhs_attr_type = lhs_attr[-1] if isinstance(lhs_attr, FCast) else None
                rhs_attr_type = rhs_attr[-1] if isinstance(rhs_attr, FCast) else None
                if lhs_attr_type != rhs_attr_type:
                    if (lhs_attr_type is not None) and (rhs_attr_type is None) \
                            and isinstance(lhs_attr_type, UN_SUPPORTED_CAST_TYPE):
                        raise NotSupportedError(f"`CAST` of {lhs_attr_type}")
                    elif (lhs_attr_type is None) and (rhs_attr_type is not None) \
                            and isinstance(rhs_attr_type, UN_SUPPORTED_CAST_TYPE):
                        raise NotSupportedError(f"`CAST` of {rhs_attr_type}")
                while utils.is_uninterpreted_func(lhs_attr) and utils.is_uninterpreted_func(rhs_attr):
                    if lhs_attr.uninterpreted_func != rhs_attr.uninterpreted_func:
                        raise NotEquivalenceError
                    else:
                        lhs_attr.uninterpreted_func = rhs_attr.uninterpreted_func = None

        equivalence_formulas = self.verifier.run(
            *tables, *result_formulas,
            lhs_tuple.attributes, rhs_tuple.attributes,
            orderby_constraints=self.orderby_constraints,
            bound_constraints=self.bound_constraints,
        )
        if self.traversing_time is not None:
            self.solving_time = time()
            self.traversing_time = round(self.solving_time - self.traversing_time, 6)
        self.solver.add(Not(equivalence_formulas))  # Not means cannot find a satisfying solution
        out = self.solver.check()
        if self.traversing_time is not None:
            self.solving_time = round(time() - self.solving_time, 6)
        LOGGER.debug(f'Symbolic Reasoning Output: ==> {out} <==')
        if out == sat:
            model = self.solver.model()
            self.counterexample = "-- ----------A counterexample found by VeriEQL------------\n"

            def _f(null, value, out_str=False, data_preix=None, type=None):
                if not isinstance(null, bool):
                    null = eval(str(model.eval(null, model_completion=True)))
                if null:
                    value = 99999
                else:
                    if not isinstance(value, int | float):
                        value = eval(str(model.eval(value, model_completion=False)))

                if value == 99999:
                    return 'NULL'
                else:
                    if out_str:
                        return f"'{value}'"
                    else:
                        value = value if data_preix is None else f"'{data_preix + str(value)}'"
                        if type == 'boolean':
                            return value != 0
                        else:
                            return value

            if self.sql_code is not None:
                if self.to_cypher:
                    nodes, edges = defaultdict(list), defaultdict(list)

                for name, basetable in self.base_databases.items():
                    insert_rows = []
                    for tuple in basetable.tuples:
                        if str(model.eval(self.DELETED_FUNCTION(tuple.SORT))) == 'True':
                            continue

                        values = []
                        for idx, attr in enumerate(basetable.attributes, start=1):
                            v = _f(attr.NULL(tuple.SORT), attr.VALUE(tuple.SORT))
                            if str.startswith(self.sql_code['tables'][attr.prefix][attr.name], "VARCHAR"):
                                if v in self.variables:
                                    v = str(self.variables[v])
                                    if v.startswith('String_'):
                                        v = v[7:]
                                    v = v.split('__')[0]
                                if v != 'NULL':
                                    v = f"\'{v}\'"
                            elif self.sql_code['tables'][attr.prefix][attr.name] == "DATE":
                                if v != 'NULL':
                                    v = f"\'{utils.int_to_strptime(v)}\'"
                            values.append(v)
                        if self.to_cypher and basetable.name[:2] in {'N_', 'E_'}:
                            if basetable.name[:2] == 'N_':
                                name = f'n{self.num_node}'
                                label = 'N$' + basetable.name[2:]
                                self.num_node += 1
                                info = {str(attr).split('__')[-1]: v for attr, v in zip(basetable.attributes, values)}
                                info_str = '{' + ', '.join([f'{attr}: {v}' for attr, v in info.items()]) + '}'
                                nodes[label].append([name, info, info_str])
                                self.cypher_counterexample.append(f"({name}:{label} {info_str})")
                            elif basetable.name[:2] == 'E_':
                                name = f'e{self.num_edge}'
                                label = 'E$' + basetable.name[2:]
                                self.num_edge += 1
                                info = {str(attr).split('__')[-1]: v for attr, v in zip(basetable.attributes, values)}
                                info_str = '{' + ', '.join([f'{attr}: {v}' for attr, v in info.items()]) + '}'
                                edges[label].append([name, info, info_str])
                        values = f"INSERT INTO {basetable.name} VALUES ({', '.join(str(v) for v in values)});\n"
                        insert_rows.append(values)
                    attr_rows = f',\n\t'.join(
                        f"{attr} {type}" for attr, type in self.sql_code['tables'][basetable.name].items())
                    self.sql_code['tables'][basetable.name] = f"CREATE TABLE {basetable.name} (\n\t{attr_rows}\n);\n"
                    self.sql_code['tables'][basetable.name] += ''.join(insert_rows)
                    self.counterexample += self.sql_code['tables'][basetable.name]

                self.counterexample += '-- ----------sql1------------\n'
                for tuple in tables[0].values():
                    if str(model.eval(self.DELETED_FUNCTION(tuple.SORT))) == 'True':
                        continue

                    values = []
                    for attr in tuple.attributes:
                        v = _f(attr.NULL(tuple.SORT), attr.VALUE(tuple.SORT))
                        if v in self.variables:
                            v = str(self.variables[v])
                            if v.startswith('String_'):
                                v = v[7:]
                            v = v.split('__')[0]
                        values.append(v)
                    self.counterexample += '-- ' + ', '.join(str(v) for v in values) + '\n'
                self.counterexample += self.sql_code['sql1'] + '\n'

                self.counterexample += '-- ----------sql2------------\n'
                for tuple in tables[1].values():
                    if str(model.eval(self.DELETED_FUNCTION(tuple.SORT))) == 'True':
                        continue

                    values = []
                    for attr in tuple.attributes:
                        v = _f(attr.NULL(tuple.SORT), attr.VALUE(tuple.SORT))
                        if v in self.variables:
                            v = str(self.variables[v])
                            if v.startswith('String_'):
                                v = v[7:]
                            v = v.split('__')[0]
                        values.append(v)
                    self.counterexample += '-- ' + ', '.join(str(v) for v in values) + '\n'
                self.counterexample += self.sql_code['sql2'] + '\n'

                if self.to_cypher:
                    for e_label, edge in edges.items():
                        for e_name, e_info, e_info_str in edge:
                            src_n = self.kwargs['graph']['schema']['edge'][e_label]['SRC']
                            direction = "->"
                            dst_n = self.kwargs['graph']['schema']['edge'][e_label]['TGT']
                            # src_n, direction, dst_n = self.kwargs['graph']['schema']['edge'][e_label]['direction']
                            src_n_attr, e_src_attr = self.kwargs['graph']['links'][(src_n, e_label)][direction]
                            src_names = []
                            for n_name, n_info, _ in nodes[src_n]:
                                if e_info[e_src_attr] == n_info[src_n_attr]:
                                    src_names.append(n_name)
                            e_dst_attr, dst_n_attr = self.kwargs['graph']['links'][(e_label, dst_n)][direction]
                            dst_names = []
                            for n_name, n_info, _ in nodes[dst_n]:
                                if e_info[e_dst_attr] == n_info[dst_n_attr]:
                                    dst_names.append(n_name)
                            if direction == '->':
                                self.cypher_counterexample += [
                                    f"({src_name})-[{e_name}_{idx}:{e_label} {e_info_str}]{direction}({dst_name})"
                                    for idx, (src_name, dst_name) in
                                    enumerate(itertools.product(src_names, dst_names), start=1)
                                ]
                            else:
                                self.cypher_counterexample += [
                                    f"({src_name}){direction}[{e_name}_{idx}:{e_label} {e_info_str}]-({dst_name})"
                                    for idx, (src_name, dst_name) in
                                    enumerate(itertools.product(src_names, dst_names), start=1)
                                ]
                    self.cypher_counterexample = '-- ----------Cypher------------\n' + \
                                                 "MATCH (n) DETACH DELETE n;\n" + \
                                                 f"CREATE {', '.join(self.cypher_counterexample)};\n" + \
                                                 self.kwargs['graph']['cypher']

            return False
        elif out == unknown:
            raise UnknownError
        else:
            return True


__all__ = [
    'Environment',
]
