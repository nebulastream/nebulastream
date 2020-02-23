#ifndef NES_INCLUDE_QUERYCOMPILER_EXECUTABLE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_EXECUTABLE_HPP_


namespace NES{

class Executable {
 public:
  virtual ~Executable();
  void execute(const TupleBuffer *input_buffers,
          void *state, WindowManager *window_manager,
          TupleBuffer *result_buf);
  template <typename Function> Function getFunctionPointer(const std::string& name)
  {
    // INFO
    // http://www.trilithium.com/johan/2004/12/problem-with-dlsym/
    // No real solution in 2016.
    static_assert(sizeof(void*) == sizeof(Function), "Void pointer to function pointer conversion will not work!"
                                                     " If you encounter this, run!");

    union converter {
      void* v_ptr;
      Function f_ptr;
    };

    converter conv;
    conv.v_ptr = getFunctionPointerImpl(name);

    return conv.f_ptr;
  }

  double getCompileTimeInSeconds() const { return compile_time_in_ns_ / double(1e9); }

 protected:
  CompiledCCode(Timestamp compile_time) : compile_time_in_ns_(compile_time) {}

  virtual void* getFunctionPointerImpl(const std::string& name) = 0;

 private:
  Timestamp compile_time_in_ns_;
};


}
#endif //NES_INCLUDE_QUERYCOMPILER_EXECUTABLE_HPP_
