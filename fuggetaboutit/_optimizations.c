#include <Python.h>
#include <numpy/arrayobject.h>
#include <stdbool.h>
#include <stdlib.h>
 
/* Docstrings */
static char module_docstring[] = "Provides fast implemintations of possibly slow functions in fuggetaboutit.  In addition, this module implements the bloom filters in 4 bits instead of 8.";
static char timing_bloom_decay_docstring[] = "Decay a timing bloom";
static char timing_bloom_contains_docstring[] = "Check if a bloom contains a key";
static char timing_bloom_add_docstring[] = "Adds a tick to a bloom";

PyObject* py_timing_bloom_add(PyObject* self, PyObject* args) {
    PyArrayObject* data;
    PyObject* indexes;
    uint8_t tick;

    if (!PyArg_ParseTuple(args, "OOB", &data, &indexes, &tick)) { 
        PyErr_SetString(PyExc_RuntimeError, "Invalid arguments");
        return NULL;
    }
    if (!PyArray_Check(data) || !PyArray_ISCONTIGUOUS(data)) {
        PyErr_SetString(PyExc_RuntimeError,"inputted data not in the correct format");
        return NULL;
    }
    if (!PyIter_Check(indexes)) {
        PyErr_SetString(PyExc_RuntimeError,"indexes argument must be an iterator");
        return NULL;
    }
    

    uint8_t *values = PyArray_DATA(data);
    uint8_t temp, n;
    int num_non_zero = 0;
    PyObject *pyindex;
    long index, access_index;

    while ((pyindex = PyIter_Next(indexes)) != NULL) {
        index = PyInt_AsLong(pyindex);
        Py_DECREF(pyindex);
        access_index = index / 2;

        n = values[access_index];
        if (index % 2 == 0) {
            temp = n & 0xf0;
            n = ((tick << 4) & 0xf0) | (n & 0x0f);
        } else {
            temp = n & 0x0f;
            n = (tick & 0x0f) | (n & 0xf0);
        }

        if (temp == 0) {
            num_non_zero += 1;
        }
        values[access_index] = n;
    }
    Py_DECREF(indexes);

    PyObject *ret = Py_BuildValue("i", num_non_zero);
    return ret;
}

PyObject* py_timing_bloom_contains(PyObject* self, PyObject* args) {
    PyArrayObject* data;
    PyObject* indexes;
    uint8_t tick_min, tick_max;

    if (!PyArg_ParseTuple(args, "OOBB", &data, &indexes, &tick_min, &tick_max)) { 
        PyErr_SetString(PyExc_RuntimeError, "Invalid arguments");
        return NULL;
    }
    if (!PyArray_Check(data) || !PyArray_ISCONTIGUOUS(data)) {
        PyErr_SetString(PyExc_RuntimeError,"inputted data not in the correct format");
        return NULL;
    }
    if (!PyIter_Check(indexes)) {
        PyErr_SetString(PyExc_RuntimeError,"indexes argument must be an iterator");
        return NULL;
    }
    

    uint8_t *values = PyArray_DATA(data);
    uint8_t value;
    bool ring_interval = (tick_max < tick_min);
    bool contains = true;
    PyObject *pyindex;
    long index, access_index;

    if (ring_interval) {
        uint8_t tmp = tick_min;
        tick_min = tick_max;
        tick_max = tmp;
    }

    while ((pyindex = PyIter_Next(indexes)) != NULL) {
        index = PyInt_AsLong(pyindex);
        Py_DECREF(pyindex);
        access_index = index / 2;
        if (index % 2 == 0) {
            value = (values[access_index] & 0xf0) >> 4;
        } else {
            value = values[access_index] & 0x0f;
        }
        if (value == 0 || ((value > tick_max || value <= tick_min) ^ ring_interval))  {
            contains = false;
            break;
        }
    }
    Py_DECREF(indexes);

    PyObject *ret = Py_BuildValue("b", (char)contains);
    return ret;
}

PyObject* py_timing_bloom_decay(PyObject* self, PyObject* args) {
    PyArrayObject* data;
    uint8_t tick_min, tick_max;

    if (!PyArg_ParseTuple(args, "OBB", &data, &tick_min, &tick_max)) { 
        PyErr_SetString(PyExc_RuntimeError, "Invalid arguments");
        return NULL;
    }
    if (!PyArray_Check(data) || !PyArray_ISCONTIGUOUS(data)) {
        PyErr_SetString(PyExc_RuntimeError,"inputted data not in the correct format");
        return NULL;
    }
    
    const int N = (int) PyArray_DIM(data, 0) * 2;
    uint8_t value;
    uint8_t *values = PyArray_DATA(data);
    int num_non_zero = 0;
    bool ring_interval = (tick_max < tick_min);
    long access_index;

    if (ring_interval) {
        uint8_t tmp = tick_min;
        tick_min = tick_max;
        tick_max = tmp;
    }

    for(long i=0; i<N; i+=2) {
        access_index = i / 2;
        value = (values[access_index] & 0xf0) >> 4;
        
        if (value != 0) {
            if ((value > tick_max || value <= tick_min) ^ ring_interval)  {
                values[access_index] &= 0x0f;
            } else {
                num_non_zero += 1;
            }
        }

        value = values[access_index] & 0x0f;
        if (value != 0) {
            if ((value > tick_max || value <= tick_min) ^ ring_interval)  {
                values[access_index] &= 0xf0;
            } else {
                num_non_zero += 1;
            }
        }
    }
    PyObject *ret = Py_BuildValue("i", num_non_zero);
    return ret;
}


/* Module specification */
static PyMethodDef module_methods[] = {
    {"timing_bloom_decay"    , py_timing_bloom_decay    , METH_VARARGS , timing_bloom_decay_docstring    }  , 
    {"timing_bloom_contains" , py_timing_bloom_contains , METH_VARARGS , timing_bloom_contains_docstring }  , 
    {"timing_bloom_add"      , py_timing_bloom_add      , METH_VARARGS , timing_bloom_add_docstring      }  , 
    {NULL                    , NULL                     , 0            , NULL                            } 
};
 
/* Initialize the module */
PyMODINIT_FUNC init_optimizations(void)
{
    PyObject *m = Py_InitModule3("_optimizations", module_methods, module_docstring);
    if (m == NULL)
        return;

    /* Load `numpy` functionality. */
    import_array();
}
