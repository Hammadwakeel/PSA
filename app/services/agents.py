import json
import logging
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types

from app.core.config import config
from app.services.rivergen_backend import RgenClient, RgenBackendError

logger = logging.getLogger(__name__)


def json_agent(prompt, model_class):
    model_name = config.GENAI_MODEL or "gemini-2.0-flash"
    logger.info("json_agent call | model=%s | schema=%s", model_name, model_class.__name__)
    client = genai.Client(api_key=config.GOOGLE_API_KEY)
    schema = model_class.model_json_schema()

    response = client.models.generate_content(
        model=config.GENAI_MODEL,
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_json_schema": schema,
        },
    )
    result = json.loads(response.text)
    logger.debug(
        "json_agent response | schema=%s | keys=%s",
        model_class.__name__,
        list(result.keys()) if isinstance(result, dict) else type(result).__name__,
    )
    return result


def _serialize_for_response(obj: Any) -> Any:
    """Make value JSON-serializable for API response."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (dict, list)):
        try:
            return json.loads(json.dumps(obj, default=str))
        except (TypeError, ValueError):
            return str(obj)
    return str(obj)


def _tool_output_str(result: Any) -> str:
    """Convert tool result to string for output field."""
    if result is None:
        return ""
    if isinstance(result, (dict, list)):
        try:
            return json.dumps(result, default=str)
        except (TypeError, ValueError):
            return str(result)
    return str(result)


def _get_response_parts(response: Any) -> List[Any]:
    """Safely get content.parts from Gemini response; return empty list if missing."""
    try:
        if not response or not getattr(response, "candidates", None):
            return []
        c0 = response.candidates[0]
        if not getattr(c0, "content", None):
            return []
        return getattr(c0.content, "parts", None) or []
    except (IndexError, AttributeError, TypeError):
        return []


def _get_function_calls(parts: List[Any]) -> List[Any]:
    """Extract function_call parts for tool execution."""
    out = []
    for p in parts or []:
        fc = getattr(p, "function_call", None)
        if fc is not None:
            out.append(fc)
    return out


def _args_from_function_call(fn: Any) -> dict:
    """Extract args dict from a function_call part."""
    args = getattr(fn, "args", None)
    if isinstance(args, dict):
        return args
    if args is None:
        return {}
    return (
        getattr(args, "__dict__", None)
        or {
            k: getattr(args, k)
            for k in dir(args)
            if not k.startswith("_") and not callable(getattr(args, k, None))
        }
        or {}
    )


def multiflow_planning(
    client: RgenClient,
    user_prompt: str,
    tools_list: List[dict],
    flow_mode: str,
) -> Dict[str, Any]:
    """
    Chat history is maintained by Gemini: we send (1) user_prompt then (2) tool
    response parts; each send_message appends to the conversation and returns
    the model reply.

    flow_mode:
      - "single": Run ALL tool calls in one round, then one send_message with
        all tool results; one response_text.
      - "multi": One tool per round: execute one tool → send_message with that
        result → get response_text; repeat.

    Returns: { "final_message": str, "steps": [ { step_index, tool_calls, response_text } ] }
    """
    model_name = config.GENAI_MODEL or "gemini-2.0-flash"
    tool_names = [t.get("name") for t in tools_list if isinstance(t, dict)]
    logger.info(
        "multiflow_planning started | model=%s | flow_mode=%s | tools=%s | prompt_len=%s",
        model_name,
        flow_mode,
        tool_names,
        len(user_prompt or ""),
    )

    try:
        genai_client = genai.Client(api_key=config.GOOGLE_API_KEY)
        chat = genai_client.chats.create(
            model=model_name,
            config=types.GenerateContentConfig(
                tools=[types.Tool(function_declarations=tools_list)],
                tool_config=types.ToolConfig(
                    function_calling_config=types.FunctionCallingConfig(mode="AUTO")
                ),
            ),
        )
    except Exception as e:
        logger.exception("multiflow_planning chat create failed | error=%s", str(e))
        return {"final_message": f"Failed to create chat: {e}", "steps": []}

    steps: List[Dict[str, Any]] = []
    step_index = 0
    last_executed_tool_name: Optional[str] = None
    last_tool_failed: bool = False

    try:
        response = chat.send_message(user_prompt)
    except Exception as e:
        logger.exception("multiflow_planning send_message(user_prompt) failed")
        return {"final_message": f"Failed to send prompt: {e}", "steps": []}

    for _ in range(10):
        parts = _get_response_parts(response)
        calls = _get_function_calls(parts)

        if not calls:
            final_text = getattr(response, "text", None) or ""
            if not final_text and response:
                final_text = str(response)
            logger.info("multiflow_planning finished (no function calls)")
            return {"final_message": final_text, "steps": steps}

        if flow_mode == "single":
            # response_text for this step = previous response (the one that triggered these tool calls)
            previous_response_text = getattr(response, "text", None) or ""
            step_index += 1
            tool_calls_record = []
            tool_responses = []
            for fn in calls:
                args = _args_from_function_call(fn)
                args_serialized = _serialize_for_response(args)
                logger.info("tool_execution [single] | tool=%s | args=%s", fn.name, args)
                try:
                    result = client.execute_tool_real(fn.name, **args)
                    output_str = _tool_output_str(result)
                    logger.info("tool_execution done | tool=%s", fn.name)
                except (RgenBackendError, Exception) as e:
                    logger.exception("tool_execution failed | tool=%s | error=%s", fn.name, str(e))
                    output_str = json.dumps({"error": str(e)})
                    result = {"error": str(e)}
                tool_calls_record.append({
                    "tool_name": fn.name,
                    "args": args_serialized,
                    "output": output_str,
                })
                tool_responses.append(
                    types.Part.from_function_response(
                        name=fn.name,
                        response={"result": result},
                    )
                )
            try:
                response = chat.send_message(tool_responses)
            except Exception as e:
                logger.exception("multiflow_planning send_message(tool_responses) failed")
                steps.append({
                    "step_index": step_index,
                    "tool_calls": tool_calls_record,
                    "response_text": previous_response_text,
                })
                return {"final_message": str(e), "steps": steps}
            steps.append({
                "step_index": step_index,
                "tool_calls": tool_calls_record,
                "response_text": previous_response_text,
            })
            return {"final_message": getattr(response, "text", None) or "", "steps": steps}

        # Multi flow: one tool per round; if previous tool failed, do not execute next tool
        previous_response_text = getattr(response, "text", None) or ""
        fn = calls[0]
        if last_tool_failed and last_executed_tool_name is not None and fn.name != last_executed_tool_name:
            logger.warning(
                "multi flow: previous tool %s failed; refusing to run next tool %s",
                last_executed_tool_name,
                fn.name,
            )
            stop_message = (
                "Stopped: previous tool failed; not proceeding to next step. "
                "Fix or retry the failed tool before calling the next one."
            )
            return {"final_message": stop_message, "steps": steps}
        args = _args_from_function_call(fn)
        args_serialized = _serialize_for_response(args)
        step_index += 1
        logger.info("tool_execution [multi] step=%s | tool=%s | args=%s", step_index, fn.name, args)
        try:
            result = client.execute_tool_real(fn.name, **args)
            output_str = _tool_output_str(result)
            last_tool_failed = isinstance(result, dict) and "error" in result
            logger.info("tool_execution done | tool=%s | failed=%s", fn.name, last_tool_failed)
        except (RgenBackendError, Exception) as e:
            logger.exception("tool_execution failed | tool=%s | error=%s", fn.name, str(e))
            output_str = json.dumps({"error": str(e)})
            result = {"error": str(e)}
            last_tool_failed = True
        last_executed_tool_name = fn.name
        tool_calls_record = [{
            "tool_name": fn.name,
            "args": args_serialized,
            "output": output_str,
        }]
        tool_responses = [
            types.Part.from_function_response(
                name=fn.name,
                response={"result": result},
            )
        ]
        try:
            response = chat.send_message(tool_responses)
        except Exception as e:
            logger.exception("multiflow_planning send_message(tool_responses) [multi] failed")
            steps.append({
                "step_index": step_index,
                "tool_calls": tool_calls_record,
                "response_text": previous_response_text,
            })
            return {"final_message": str(e), "steps": steps}
        steps.append({
            "step_index": step_index,
            "tool_calls": tool_calls_record,
            "response_text": previous_response_text,
        })

    final_text = getattr(response, "text", None) or "Planning exceeded maximum steps."
    logger.warning("multiflow_planning exceeded max steps (10)")
    return {"final_message": final_text, "steps": steps}
