import json
import os
from collections.abc import Generator
from io import BytesIO
from typing import Any

import pandas as pd
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class Excel2JsonTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        file_meta = tool_parameters["file"]
        try:
            # Multiple sheets: Read all sheets into a dictionary of DataFrames
            excel_source = self._resolve_excel_source(file_meta)
            all_sheets_data = pd.read_excel(excel_source, sheet_name=None, dtype=str)
            json_output = {}
            for sheet_name, df in all_sheets_data.items():
                # Convert DataFrame to a list of records (dicts)
                json_output[sheet_name] = json.loads(
                    df.to_json(orient="records", force_ascii=False)
                )

            # Create a JSON message with the entire structure
            yield self.create_json_message(json_output)
            # Convert the entire structure to a JSON string
            # yield self.create_text_message(
            #     json.dumps(json_output, ensure_ascii=False, indent=2)
            # )

        except Exception as e:
            raise Exception(f"Error processing Excel file: {str(e)}")

    def _resolve_excel_source(self, file_meta: Any) -> Any:
        """
        Resolve a usable Excel source from upload metadata.

        Priority:
        1) Local path fields when present and existing
        2) File URL (including file:// or absolute paths)
        3) In-memory blob/bytes if provided
        """
        path_candidates = [
            getattr(file_meta, "path", None),
            getattr(file_meta, "local_path", None),
            getattr(file_meta, "file_path", None),
        ]
        for path in path_candidates:
            if path and os.path.exists(path):
                return path

        url = getattr(file_meta, "url", None)
        if isinstance(url, str) and url:
            if url.startswith("file://"):
                local_path = url[7:]
                if os.path.exists(local_path):
                    return local_path
            if os.path.isabs(url) and os.path.exists(url):
                return url
            return url

        blob = getattr(file_meta, "blob", None)
        if blob:
            return BytesIO(blob)

        raise Exception("No readable file source found in upload metadata.")
