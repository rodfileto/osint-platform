type InputFieldProps = {
  type?: string;
  id?: string;
  name?: string;
  placeholder?: string;
  value?: string | number;
  defaultValue?: string | number;
  onChange?: (event: React.ChangeEvent<HTMLInputElement>) => void;
  className?: string;
  min?: string;
  max?: string;
  step?: number;
  disabled?: boolean;
  success?: boolean;
  error?: boolean;
  hint?: string;
};

export default function InputField({
  type = "text",
  id,
  name,
  placeholder,
  value,
  defaultValue,
  onChange,
  className = "",
  min,
  max,
  step,
  disabled = false,
  success = false,
  error = false,
  hint,
}: InputFieldProps) {
  let inputClasses = `h-11 w-full appearance-none rounded-lg border px-4 py-2.5 text-sm shadow-theme-xs placeholder:text-gray-400 focus:outline-hidden focus:ring-3 dark:bg-gray-900 dark:text-white/90 dark:placeholder:text-white/30 dark:focus:border-brand-800 ${className}`;

  if (disabled) {
    inputClasses += " cursor-not-allowed border-gray-300 text-gray-500 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-400";
  } else if (error) {
    inputClasses += " border-red-500 text-red-800 focus:ring-red-500/10 dark:border-red-500 dark:text-red-400";
  } else if (success) {
    inputClasses += " border-emerald-400 text-emerald-600 focus:border-emerald-300 focus:ring-emerald-500/10 dark:border-emerald-500 dark:text-emerald-400";
  } else {
    inputClasses += " border-gray-300 bg-transparent text-gray-800 focus:border-brand-300 focus:ring-brand-500/10 dark:border-gray-700 dark:text-white/90";
  }

  return (
    <div className="relative">
      <input
        type={type}
        id={id}
        name={name}
        placeholder={placeholder}
        value={value}
        defaultValue={value !== undefined ? undefined : defaultValue}
        onChange={onChange}
        min={min}
        max={max}
        step={step}
        disabled={disabled}
        className={inputClasses}
      />

      {hint ? (
        <p className={`mt-1.5 text-xs ${error ? "text-red-500" : success ? "text-emerald-500" : "text-gray-500"}`}>
          {hint}
        </p>
      ) : null}
    </div>
  );
}