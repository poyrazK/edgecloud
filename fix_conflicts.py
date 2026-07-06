import re

def fix_dispatch():
    with open('edge-worker/src/dispatch.rs', 'r') as f:
        content = f.read()
    
    # We want both socket_mode and last_request_at
    content = re.sub(
        r'<<<<<<< HEAD\n(.*?)=======\n(.*?)\n>>>>>>> feature/scale-to-zero',
        r'\1\2',
        content,
        flags=re.DOTALL
    )
    with open('edge-worker/src/dispatch.rs', 'w') as f:
        f.write(content)

def fix_supervisor():
    with open('edge-worker/src/supervisor.rs', 'r') as f:
        content = f.read()
    
    # Same
    content = re.sub(
        r'<<<<<<< HEAD\n(.*?)=======\n(.*?)\n>>>>>>> feature/scale-to-zero',
        r'\1\2',
        content,
        flags=re.DOTALL
    )
    with open('edge-worker/src/supervisor.rs', 'w') as f:
        f.write(content)

def fix_layer():
    with open('edge-worker/tests/layer_integration.rs', 'r') as f:
        content = f.read()
    
    # Same
    content = re.sub(
        r'<<<<<<< HEAD\n(.*?)=======\n(.*?)\n>>>>>>> feature/scale-to-zero',
        r'\1\2',
        content,
        flags=re.DOTALL
    )
    # The first side defines metrics_acc: None and socket_mode
    # The second side defines metrics_acc: None and last_request_at
    # We want all three!
    # \1 will have metrics_acc: None and socket_mode
    # \2 will have metrics_acc: None and last_request_at
    # This will result in TWO metrics_acc: None, we should remove the duplicate.
    content = content.replace('metrics_acc: None,\n        metrics_acc: None,', 'metrics_acc: None,')
    content = content.replace('metrics_acc: Some(metrics_acc),\n        metrics_acc: Some(metrics_acc),', 'metrics_acc: Some(metrics_acc),')
    
    with open('edge-worker/tests/layer_integration.rs', 'w') as f:
        f.write(content)

fix_dispatch()
fix_supervisor()
fix_layer()
