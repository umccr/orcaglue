# Usage:
#   source dx.sh
#   undo-aws

undo-aws() {
    # undo the granted assume exported env vars in the current shell
    env | grep AWS_ | cut -f1 -d "=" | xargs
    unset `env | grep AWS_ | cut -f1 -d "="`
    # unset AWS_PROFILE AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CREDENTIAL_EXPIRATION
}
alias de-aws=undo-aws
alias deaws=undo-aws
alias rmaws=undo-aws
