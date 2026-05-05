"""Linear API client for posting comments to issues."""

import os
import requests
from typing import Optional


class LinearClient:
    """Linear GraphQL client for issue operations."""

    API_URL = "https://api.linear.app/graphql"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Linear client.

        Args:
            api_key: Linear API key. If not provided, reads from LINEAR_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("LINEAR_API_KEY")
        if not self.api_key:
            raise ValueError(
                "LINEAR_API_KEY not set. Get it from Linear workspace → Settings → API."
            )

    def _headers(self) -> dict:
        """Build headers for Linear GraphQL requests."""
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

    def post_comment(self, issue_id: str, body: str) -> bool:
        """
        Post a comment to a Linear issue.

        Args:
            issue_id: Linear issue ID (e.g., "GMARK-748")
            body: Comment body (markdown supported)

        Returns:
            True if successful, False otherwise.
        """
        mutation = """
            mutation CreateComment($input: CommentCreateInput!) {
                commentCreate(input: $input) {
                    comment {
                        id
                        body
                    }
                    success
                }
            }
        """

        variables = {
            "input": {
                "issueId": issue_id,
                "body": body,
            }
        }

        payload = {
            "query": mutation,
            "variables": variables,
        }

        try:
            resp = requests.post(
                self.API_URL,
                json=payload,
                headers=self._headers(),
                timeout=10,
            )
            resp.raise_for_status()

            data = resp.json()
            if "errors" in data:
                print(f"Linear GraphQL error posting comment: {data['errors']}")
                return False

            result = data.get("data", {}).get("commentCreate", {})
            success = result.get("success", False)
            if success:
                print(f"Linear comment posted to {issue_id}")
            else:
                print(f"Linear comment failed (success=False): {result}")
            return success

        except requests.exceptions.RequestException as e:
            print(f"Error posting Linear comment: {e}")
            return False
