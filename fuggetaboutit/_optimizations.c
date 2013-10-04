#include <Python.h>
#include <numpy/arrayobject.h>
#include <stdbool.h>
#include <stdlib.h>
 
/* Docstrings */
static char module_docstring[] = "Provides fast implemintations of possibly slow functions in fuggetaboutit";
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
    int num_non_zero = 0;
    PyObject *pyindex;
    long index;

    while ((pyindex = PyIter_Next(indexes)) != NULL) {
        index = PyInt_AsLong(pyindex);
        Py_DECREF(pyindex);
        if (values[index] == 0) {
            num_non_zero += 1;
        }
        values[index] = tick;
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
    long index;

    while ((pyindex = PyIter_Next(indexes)) != NULL) {
        index = PyInt_AsLong(pyindex);
        value = (uint8_t)values[index];
        Py_DECREF(pyindex);
        if (value == 0 || (!ring_interval && !(value > tick_min && value <= tick_max)) || (ring_interval && !(value > tick_min || value <= tick_max))) {
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
    
    const int N = (int) PyArray_DIM(data, 0);
    uint8_t value;
    uint8_t *values = PyArray_DATA(data);
    int num_non_zero = 0;
    bool ring_interval = (tick_max < tick_min);

    #pragma omp parallel for if(N > 1e6) reduction(+:num_non_zero)
    for(int i=0; i<N; i++) {
        value = (uint8_t)values[i];
        if (value != 0 && ((!ring_interval && !(value > tick_min && value <= tick_max)) || (ring_interval && !(value > tick_min || value <= tick_max)))) {
            values[i] = 0;
        } else {
            num_non_zero += 1;
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
