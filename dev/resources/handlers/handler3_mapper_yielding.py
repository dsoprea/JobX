import math

# Number of yielded steps.
yield 1

yielded_step_name = 'step1'
yielded_step_args = {'arg1': math.sqrt(arg1), 'arg2': 10.0}

yield (yielded_step_name, yielded_step_args)
