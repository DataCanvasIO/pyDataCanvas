pyDataCanvas
============

Runtime support for DataCanvas

Installation
------------

Our latest stable is always available on PyPi.

    pip install pyDataCanvas

Documentation
-------------

See also [screwjack](screwjack.readthedocs.org).

License
-------
pyDataCanvas is licensed under the Apache License, Version 2.0. See LICENSE for full license text


Kerberos Authentication (Option)
-------
* kerberosConfig:

```json
{
  "kdcLocation": "your_ldpc_server:88",
  "kServer": "your_kerberos_server:749",
  "kUserName": "your_name@TEST.COM",
  "keyTabServer": "your_keytab_server",
  "keyTabPort": 18185
}
```

* isValidate: true
