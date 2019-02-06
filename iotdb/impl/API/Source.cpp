#include <API/Source.hpp>

namespace iotdb{
Source& Source::inputType(InputType pType)
{
	typeValue = pType;
	return *this;
}

Source& Source::sourceType(SourceType sType)
{
	srcType = sType;
	return *this;
}


Source& Source::path(string pPath)
{
	pathValue = pPath;
	return *this;
}


InputType& Source::getType()
{
	return typeValue;
}


string& Source::getPath()
{
	return pathValue;
}

Source::Source()
{
//	InputType
	typeValue = InputType();
	pathValue = "";
}

Source Source::create()
{
	return Source();
}
}

