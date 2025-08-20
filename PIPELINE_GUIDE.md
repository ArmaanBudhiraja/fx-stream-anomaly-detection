# Pipeline Selection Guide

## 🤔 Which Pipeline Should You Run?

You have **two pipeline options** in this project. Here's how to choose:

## 🎯 **RECOMMENDED: Working Pipeline**

**File:** `run_working_pipeline.py`

**When to use:**
- ✅ **Most users** (especially beginners)
- ✅ **Development and testing**
- ✅ **When you want something that just works**
- ✅ **When you don't need full Kafka streaming**

**What it does:**
- Starts Docker infrastructure (Kafka + PostgreSQL)
- Processes FX data **directly** (bypasses Kafka consumer issues)
- Detects anomalies in real-time
- Stores results in database
- Starts web dashboard at http://localhost:5000

**Command:**
```bash
python run_working_pipeline.py
```

---

## ⚠️ **ADVANCED: Full Pipeline**

**File:** `run_full_pipeline.py`

**When to use:**
- ⚠️ **Advanced users only**
- ⚠️ **Production environments**
- ⚠️ **When you need complete Kafka streaming**
- ⚠️ **When you understand Kafka architecture**

**What it does:**
- Starts complete Kafka streaming pipeline
- Uses Kafka producer → Kafka topic → Kafka consumer
- More complex architecture
- May have connectivity issues if Kafka isn't properly configured

**Command:**
```bash
python run_full_pipeline.py
```

---

## 🚀 **EASIEST WAY: Use the Main Runner**

**File:** `run_pipeline.py`

**What it does:**
- Shows you a clear menu
- Explains the differences
- Helps you choose the right option
- Runs the selected pipeline

**Command:**
```bash
python run_pipeline.py
```

---

## 📋 **Quick Decision Tree**

```
Are you new to this project?
├─ YES → Use run_working_pipeline.py
└─ NO → Do you need full Kafka streaming?
    ├─ YES → Use run_full_pipeline.py
    └─ NO → Use run_working_pipeline.py
```

## 🔧 **Troubleshooting**

**If you get Kafka errors:**
- Switch to `run_working_pipeline.py`
- It bypasses Kafka consumer issues

**If you want to learn:**
- Start with `run_working_pipeline.py`
- Then try `run_full_pipeline.py` when comfortable

**If you're in production:**
- Use `run_full_pipeline.py` for full streaming
- Ensure Kafka is properly configured

---

## 📞 **Need Help?**

1. **Start with:** `python run_pipeline.py`
2. **Choose option 1** (Working Pipeline)
3. **If that works**, you're all set!
4. **If you need more**, try option 2 (Full Pipeline)

The working pipeline is designed to be reliable and get you up and running quickly! 🎉 