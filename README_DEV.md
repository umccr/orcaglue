# Development

We expect the following dev tools to be installed and available in your system PATH. We provide [Brewfile](Brewfile) as an example on macOS and you can run `brew bundle` to install them. You can manage these dev tools in any other way as see fit for your local dev setup and suits to your OS.

Tools:
- Python3 _(pick your preferred way to manage the virtual environment)_
- aws-cli
- Makefile _(Optional `make` binary to execute [Makefile](Makefile) targets. You can directly call those commands and scripts, otherwise.)_

Example:

From the project root directory, do like so.
```
conda create -n orcaglue python=3.11
conda activate orcaglue
pip3 install -r requirements-dev.txt
```

## Python Version

```
Python -V
Python 3.11.15
```

Do not over-leverage the latest Python version and any packages thereof. 

Our target runtime environment is AWS Glue and, as long as it falls within Python LTS releases.

* https://docs.aws.amazon.com/glue/latest/dg/release-notes.html
* https://docs.aws.amazon.com/glue/latest/dg/glue-version-support-policy.html
* https://www.python.org/downloads/

## AWS Session

Use your usual way of AWS CLI setup to authenticate. Manage and switch authenticated session and AWS profile however you like. Be consistent with your setup in your way. You do not need to change that. Any README.md guideline "step" around AWS CLI authenticated session is just "an example" only. The step only _signals_ that you need to be authenticated at the point. How is – up to you.

## Skills

Please do read all the documentation at https://github.com/umccr/orcahouse-doc


Dev / Infra:

- Python
- SQL
- AWS Athena
- AWS Glue
- AWS S3 datalake
- Git and GitHub
- Database Administration—DBA (query pref, tuning, backup, snapshot, proxy, tunnel, etc.)


IDE:

_note; recommendation only. leverage any other IDE combo as see fit for your productivity._

- VSCode
- JetBrains (PyCharm, DataGrip)
