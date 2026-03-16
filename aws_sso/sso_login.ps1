# Sign in to AWS via SSO for the swb-321 profile.
# Prerequisite: In ~/.aws/config, replace YOUR_SSO_START_URL with your real URL
#   (IAM Identity Center -> Dashboard -> Identity center URL).
# Then run this script (or: aws sso login --profile swb-321)
aws sso login --profile swb-321
