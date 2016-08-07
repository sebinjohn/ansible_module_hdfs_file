#!/usr/bin/env python

from ansible.module_utils.basic import AnsibleModule
from snakebite.client import AutoConfigClient


def main():
    module = AnsibleModule(
        argument_spec=dict(
            path=dict(required=True, type='str'),
            kerberos=dict(required=True, type='bool'),
            state=dict(required=True, choices=['present', 'absent', 'directory'], type='str')
        ),
        supports_check_mode=True
    )

    path = module.params.get('path')
    state = module.params.get('state')
    kerberos = module.params.get('kerberos')
    full_path = check_path(path)
    if full_path is None:
        module.fail_json('invalid paths : {}'.format(path))
    try:
        client = AutoConfigClient(use_sasl=kerberos)
        exists = client.test(full_path, exists=True)
    except Exception as e:
        module.fail_json(msg=e.message)

    if state == 'present':
        # file is assumed
        if module.check_mode:
            if exists:
                module.exit_json(changed=False, 'file already exists')
            else:
                module.exit_json(changed=True, 'file is created')
        else:
            if exists:
                module.exit_json(changed=False, msg='{0} already exists'.format(full_path))
            else:
                try:
                    x = client.touchz([full_path])
                    for i in x:
                        if i['result'] is True:
                            module.exit_json(changed=True, msg='{0} touched '.format(full_path))
                        else:
                            module.fail_json(msg='{0} creation failed'.format(full_path))
                except Exception as e:
                    module.fail_json(msg=e.message)

    if state == 'directory':
        folder_exist = client.test(full_path, exists=True, directory=True)
        if folder_exist:
            module.exit_json(changed=False, msg='hdfs directory {0} already exists'.format(full_path))

        try:
            x = client.mkdir([full_path], create_parent=False)
            for i in x:
                if i['result'] is True:
                    module.exit_json(changed=True, msg='created hdfs directory {0}'.format(full_path))
                else:
                    module.fail_json(msg='directory creation failed {0}'.format(full_path))
        except Exception as e:
            module.fail_json(msg=e.message)

    if state == 'absent':
        exists = client.test(full_path, exists=True)
        if not exists:
            module.exit_json(changed=False, msg='File or directory is absent {0}'.format(
                full_path
            ))

        is_dir = client.test(full_path, directory=True)
        if not is_dir:
            try:
                x = client.delete([full_path])
                for i in x:
                    if i['result'] is True:
                        module.exit_json(changed=True, msg='hdfs file {0} is absent'.format(full_path))
                    elif i['result'] is True:
                        module.fail_json(msg='error while deleting hdfs file'.format(full_path))
            except Exception as e:
                module.fail_json(msg=e.message)
        else:
            try:
                x = client.delete([full_path], recurse=True)
                for i in x:
                    if i['result'] is True:
                        module.exit_json(changed=True, msg='hdfs file {0} is absent'.format(full_path))
                    elif i['result'] is True:
                        module.fail_json(msg='error while deleting hdfs file'.format(full_path))
            except Exception as e:
                module.fail_json(msg=e.message)


def check_path(name):
    return name


if __name__ == '__main__':
    main()
