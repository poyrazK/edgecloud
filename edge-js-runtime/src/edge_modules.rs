//! Register edge:cloud/* WIT interfaces as QuickJS globalThis.EdgeCloud methods.

use rquickjs::{Ctx, Function, Object, TypedArray, Value};

// The wit-bindgen generated bindings are available via crate::{edge, wasi, ...}
use crate::edge::cloud::{cache, kv_store, observe, process, scheduling, time};

/// Register all edge:cloud modules on globalThis.EdgeCloud.
pub fn register_all<'js>(ctx: &Ctx<'js>) -> rquickjs::Result<()> {
    let edge_cloud = Object::new(ctx.clone())?;

    register_kv_store(ctx, &edge_cloud)?;
    register_cache(ctx, &edge_cloud)?;
    register_observe(ctx, &edge_cloud)?;
    register_time(ctx, &edge_cloud)?;
    register_scheduling(ctx, &edge_cloud)?;
    register_process(ctx, &edge_cloud)?;

    ctx.globals().set("EdgeCloud", edge_cloud)?;
    Ok(())
}

// ─── Helpers ────────────────────────────────────────────────────────

fn js_to_tuple_vec<'js>(val: Value<'js>) -> rquickjs::Result<Vec<(String, String)>> {
    let array = match val.into_array() {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };
    let mut vec = Vec::with_capacity(array.len());
    for item in array.iter() {
        let item: Value<'js> = item?;
        if let Some(pair) = item.as_array() {
            if pair.len() >= 2 {
                let k: String = pair.get(0)?;
                let v: String = pair.get(1)?;
                vec.push((k, v));
            }
        }
    }
    Ok(vec)
}

fn tuple_vec_to_js<'js>(ctx: &Ctx<'js>, vec: Vec<(String, String)>) -> rquickjs::Result<rquickjs::Array<'js>> {
    let arr = rquickjs::Array::new(ctx.clone())?;
    for (i, (k, v)) in vec.into_iter().enumerate() {
        let pair = rquickjs::Array::new(ctx.clone())?;
        pair.set(0, k)?;
        pair.set(1, v)?;
        arr.set(i, pair)?;
    }
    Ok(arr)
}

fn js_to_set_many_items<'js>(val: Value<'js>) -> rquickjs::Result<Vec<(String, Vec<u8>, Option<u32>)>> {
    let array = match val.into_array() {
        Some(arr) => arr,
        None => return Ok(Vec::new()),
    };
    let mut vec = Vec::with_capacity(array.len());
    for item in array.iter() {
        let item: Value<'js> = item?;
        if let Some(tuple) = item.as_array() {
            if tuple.len() >= 2 {
                let k: String = tuple.get(0)?;
                let v_val: Value<'js> = tuple.get(1)?;
                let v: Vec<u8> = if let Ok(ta) = TypedArray::<'js, u8>::from_value(v_val) {
                    let bytes: &[u8] = ta.as_ref();
                    bytes.to_vec()
                } else {
                    Vec::new()
                };
                let ttl: Option<u32> = if tuple.len() >= 3 {
                    tuple.get(2)?
                } else {
                    None
                };
                vec.push((k, v, ttl));
            }
        }
    }
    Ok(vec)
}

// ─── kv-store ──────────────────────────────────────────────────────

fn register_kv_store<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let kv = Object::new(ctx.clone())?;

    kv.set("get", Function::new(ctx.clone(), |ctx: Ctx<'js>, key: String| -> rquickjs::Result<Value<'js>> {
        match kv_store::get(&key) {
            Some(bytes) => {
                let ta = TypedArray::new(ctx, bytes)?;
                Ok(ta.into_value())
            }
            None => Ok(Value::new_null(ctx)),
        }
    }))?;

    kv.set("set", Function::new(ctx.clone(), |value_val: Value<'js>, key: String, ttl: Option<u32>| -> rquickjs::Result<()> {
        let value = TypedArray::<'js, u8>::from_value(value_val)?;
        let bytes: &[u8] = value.as_ref();
        kv_store::set(&key, bytes, ttl);
        Ok(())
    }))?;

    kv.set("delete", Function::new(ctx.clone(), |key: String| {
        kv_store::delete(&key);
    }))?;

    kv.set("listKeys", Function::new(ctx.clone(), |prefix: String| -> Vec<String> {
        kv_store::list_keys(&prefix)
    }))?;

    kv.set("getMany", Function::new(ctx.clone(), |ctx: Ctx<'js>, keys: Vec<String>| -> rquickjs::Result<Vec<Value<'js>>> {
        let results = kv_store::get_many(&keys);
        let mut js_results = Vec::with_capacity(results.len());
        for opt in results {
            match opt {
                Some(bytes) => {
                    let ta = TypedArray::new(ctx.clone(), bytes)?;
                    js_results.push(ta.into_value());
                }
                None => js_results.push(Value::new_null(ctx.clone())),
            }
        }
        Ok(js_results)
    }))?;

    kv.set("setMany", Function::new(ctx.clone(), |items_val: Value<'js>| -> rquickjs::Result<()> {
        let items = js_to_set_many_items(items_val)?;
        kv_store::set_many(&items);
        Ok(())
    }))?;

    kv.set("deleteMany", Function::new(ctx.clone(), |keys: Vec<String>| {
        kv_store::delete_many(&keys);
    }))?;

    kv.set("exists", Function::new(ctx.clone(), |key: String| -> bool {
        kv_store::exists(&key)
    }))?;

    kv.set("clear", Function::new(ctx.clone(), || {
        kv_store::clear();
    }))?;

    parent.set("kv", kv)?;
    Ok(())
}

// ─── cache ──────────────────────────────────────────────────────────

fn register_cache<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let c = Object::new(ctx.clone())?;

    c.set("get", Function::new(ctx.clone(), |ctx: Ctx<'js>, key: String| -> rquickjs::Result<Value<'js>> {
        match cache::get(&key) {
            Some(bytes) => {
                let ta = TypedArray::new(ctx, bytes)?;
                Ok(ta.into_value())
            }
            None => Ok(Value::new_null(ctx)),
        }
    }))?;

    c.set("set", Function::new(ctx.clone(), |value_val: Value<'js>, key: String, ttl: Option<u32>| -> rquickjs::Result<()> {
        let value = TypedArray::<'js, u8>::from_value(value_val)?;
        let bytes: &[u8] = value.as_ref();
        cache::set(&key, bytes, ttl);
        Ok(())
    }))?;

    c.set("delete", Function::new(ctx.clone(), |key: String| {
        cache::delete(&key);
    }))?;

    c.set("clear", Function::new(ctx.clone(), || {
        cache::clear();
    }))?;

    c.set("size", Function::new(ctx.clone(), || -> u32 {
        cache::size()
    }))?;

    c.set("exists", Function::new(ctx.clone(), |key: String| -> bool {
        cache::exists(&key)
    }))?;

    c.set("listKeys", Function::new(ctx.clone(), |prefix: String| -> Vec<String> {
        cache::list_keys(&prefix)
    }))?;

    c.set("getMany", Function::new(ctx.clone(), |ctx: Ctx<'js>, keys: Vec<String>| -> rquickjs::Result<Vec<Value<'js>>> {
        let results = cache::get_many(&keys);
        let mut js_results = Vec::with_capacity(results.len());
        for opt in results {
            match opt {
                Some(bytes) => {
                    let ta = TypedArray::new(ctx.clone(), bytes)?;
                    js_results.push(ta.into_value());
                }
                None => js_results.push(Value::new_null(ctx.clone())),
            }
        }
        Ok(js_results)
    }))?;

    c.set("setMany", Function::new(ctx.clone(), |items_val: Value<'js>| -> rquickjs::Result<()> {
        let items = js_to_set_many_items(items_val)?;
        cache::set_many(&items);
        Ok(())
    }))?;

    c.set("deleteMany", Function::new(ctx.clone(), |keys: Vec<String>| {
        cache::delete_many(&keys);
    }))?;

    parent.set("cache", c)?;
    Ok(())
}

// ─── observe ────────────────────────────────────────────────────────

fn register_observe<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let obs = Object::new(ctx.clone())?;

    obs.set("incrementCounter", Function::new(ctx.clone(), |name: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
        let labels = js_to_tuple_vec(labels_val)?;
        observe::increment_counter(&name, &labels);
        Ok(())
    }))?;

    obs.set("recordGauge", Function::new(ctx.clone(), |name: String, value: f64, labels_val: Value<'js>| -> rquickjs::Result<()> {
        let labels = js_to_tuple_vec(labels_val)?;
        observe::record_gauge(&name, value, &labels);
        Ok(())
    }))?;

    obs.set("recordHistogram", Function::new(ctx.clone(), |name: String, value: f64, labels_val: Value<'js>| -> rquickjs::Result<()> {
        let labels = js_to_tuple_vec(labels_val)?;
        observe::record_histogram(&name, value, &labels);
        Ok(())
    }))?;

    obs.set("emitLog", Function::new(ctx.clone(), |level: String, message: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
        let labels = js_to_tuple_vec(labels_val)?;
        observe::emit_log(&level, &message, &labels);
        Ok(())
    }))?;

    obs.set("emitLogRecord", Function::new(ctx.clone(), |timestamp_ms: u64, level: String, message: String, labels_val: Value<'js>| -> rquickjs::Result<()> {
        let labels = js_to_tuple_vec(labels_val)?;
        let lvl = match level.as_str() {
            "error" => observe::LogLevel::Error,
            "warn" => observe::LogLevel::Warn,
            "info" => observe::LogLevel::Info,
            "debug" => observe::LogLevel::Debug,
            _ => observe::LogLevel::Trace,
        };
        observe::emit_log_record(&observe::LogRecord {
            timestamp_ms,
            level: lvl,
            message,
            labels,
        });
        Ok(())
    }))?;

    parent.set("observe", obs)?;
    Ok(())
}

// ─── time ───────────────────────────────────────────────────────────

fn register_time<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let t = Object::new(ctx.clone())?;

    t.set("now", Function::new(ctx.clone(), || -> u64 {
        time::now()
    }))?;

    t.set("sleep", Function::new(ctx.clone(), |duration_ms: u64| {
        time::sleep(duration_ms);
    }))?;

    t.set("resolution", Function::new(ctx.clone(), || -> u64 {
        time::resolution()
    }))?;

    parent.set("time", t)?;
    Ok(())
}

// ─── scheduling ─────────────────────────────────────────────────────

fn register_scheduling<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let s = Object::new(ctx.clone())?;

    s.set("scheduleOnce", Function::new(ctx.clone(), |delay_ms: u64, payload_val: Value<'js>| -> rquickjs::Result<String> {
        let payload = TypedArray::<'js, u8>::from_value(payload_val)?;
        let bytes: &[u8] = payload.as_ref();
        Ok(scheduling::schedule_once(delay_ms, bytes))
    }))?;

    s.set("scheduleRepeating", Function::new(ctx.clone(), |interval_ms: u64, payload_val: Value<'js>| -> rquickjs::Result<String> {
        let payload = TypedArray::<'js, u8>::from_value(payload_val)?;
        let bytes: &[u8] = payload.as_ref();
        Ok(scheduling::schedule_repeating(interval_ms, bytes))
    }))?;

    s.set("cancelScheduled", Function::new(ctx.clone(), |id: String| {
        scheduling::cancel_scheduled(&id);
    }))?;

    parent.set("scheduling", s)?;
    Ok(())
}

// ─── process ────────────────────────────────────────────────────────

fn register_process<'js>(ctx: &Ctx<'js>, parent: &Object<'js>) -> rquickjs::Result<()> {
    let p = Object::new(ctx.clone())?;

    p.set("getEnv", Function::new(ctx.clone(), |key: String| -> Option<String> {
        process::get_env(&key)
    }))?;

    p.set("getAllEnv", Function::new(ctx.clone(), |ctx: Ctx<'js>| -> rquickjs::Result<rquickjs::Array<'js>> {
        let envs = process::get_all_env();
        tuple_vec_to_js(&ctx, envs)
    }))?;

    p.set("getArgs", Function::new(ctx.clone(), || -> Vec<String> {
        process::get_args()
    }))?;

    p.set("getCwd", Function::new(ctx.clone(), |ctx: Ctx<'js>| -> rquickjs::Result<Value<'js>> {
        match process::get_cwd() {
            Ok(cwd) => {
                let obj = Object::new(ctx.clone())?;
                obj.set("ok", cwd)?;
                Ok(obj.into_value())
            }
            Err(err) => {
                let obj = Object::new(ctx.clone())?;
                obj.set("err", err)?;
                Ok(obj.into_value())
            }
        }
    }))?;

    p.set("exit", Function::new(ctx.clone(), |code: u32| {
        process::exit(code);
    }))?;

    parent.set("process", p)?;
    Ok(())
}
