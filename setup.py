import setuptools
import os.path

import mr

_APP_PATH = os.path.dirname(mr.__file__)

with open(os.path.join(_APP_PATH, 'resources', 'README.rst')) as f:
      long_description = f.read()

with open(os.path.join(_APP_PATH, 'resources', 'requirements.txt')) as f:
      install_requires = [s.strip() for s in f.readlines()]

setuptools.setup(
    name='mapreduce',
    version=mr.__version__,
    description="A Python-based, distributed MapReduce solution.",
    long_description=long_description,
    classifiers=[],
    keywords='mapreduce jobx jobs workers',
    author='Dustin Oprea',
    author_email='myselfasunder@gmail.com',
    url='https://github.com/dsoprea/JobX',
    license='GPL 2',
    packages=setuptools.find_packages(exclude=['dev']),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    package_data={
        'mr': [
            'resources/README.rst',
            'resources/requirements.txt',
            'resources/data/gunicorn_conf_dev.py',
            'resources/data/gunicorn_conf_prod.py'
        ],
    },
    scripts=[
        'mr/resources/scripts/mr_draw_invocation_graph',
        'mr/resources/scripts/mr_get_invocation_flat_list',
        'mr/resources/scripts/mr_get_invocation_graph',
        'mr/resources/scripts/mr_get_request_sessions',
        'mr/resources/scripts/mr_kv_handler_create',
        'mr/resources/scripts/mr_kv_handler_delete',
        'mr/resources/scripts/mr_kv_handler_get',
        'mr/resources/scripts/mr_kv_handler_list',
        'mr/resources/scripts/mr_kv_handler_store',
        'mr/resources/scripts/mr_kv_invocation_create',
        'mr/resources/scripts/mr_kv_invocation_get',
        'mr/resources/scripts/mr_kv_job_create',
        'mr/resources/scripts/mr_kv_job_delete',
        'mr/resources/scripts/mr_kv_job_get',
        'mr/resources/scripts/mr_kv_job_list',
        'mr/resources/scripts/mr_kv_request_cleanup',
        'mr/resources/scripts/mr_kv_request_create',
        'mr/resources/scripts/mr_kv_request_get',
        'mr/resources/scripts/mr_kv_step_create',
        'mr/resources/scripts/mr_kv_step_delete',
        'mr/resources/scripts/mr_kv_step_get',
        'mr/resources/scripts/mr_kv_step_list',
        'mr/resources/scripts/mr_kv_step_set_handler',
        'mr/resources/scripts/mr_kv_step_update_handlers',
        'mr/resources/scripts/mr_kv_t_mapped_steps_add',
        'mr/resources/scripts/mr_kv_t_mapped_steps_create.obs',
        'mr/resources/scripts/mr_kv_t_mapped_steps_get.obs',
        'mr/resources/scripts/mr_kv_workflow_create',
        'mr/resources/scripts/mr_kv_workflow_delete',
        'mr/resources/scripts/mr_kv_workflow_get',
        'mr/resources/scripts/mr_start_gunicorn_dev',
        'mr/resources/scripts/mr_start_gunicorn_prod',
    ],
)
