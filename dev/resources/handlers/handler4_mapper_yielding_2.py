import random

interval = random.randrange(0, arg1)
count = arg1 // interval

# Number of yielded steps.
#yield count

for i in range(count):
    yielded_step_name = 'step5'
    yielded_step_args = {'arg1': interval }

    yield (yielded_step_name, yielded_step_args)
